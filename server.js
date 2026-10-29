const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mongoose = require('mongoose');
const cron = require('node-cron');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(bodyParser.json());

// Conexión a MongoDB Atlas
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://estacion_iot:ek2ZiLoeaHDZm9P0@estacion.0vujvgu.mongodb.net/estacion_iot?retryWrites=true&w=majority";

mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 })
    .then(() => console.log("-> ¡Conectado a MongoDB Atlas!"))
    .catch(err => console.error("-> Error de conexión:", err.message));

// ==========================================
// ESQUEMAS Y MODELOS DE BASE DE DATOS
// ==========================================

// 1. Alta Definición (Tiempo Real - Cada 5 min del día)
const lecturaSchema = new mongoose.Schema({
    temperature: Number,
    humidity: Number,
    pressure: Number,
    pm25: Number,
    co2: Number,
    mq7: Number,
    ldr: Number,
    uv: Number,
    airQuality: Number,
    latitude: Number,
    longitude: Number,
    timestamp: { type: Date, default: Date.now }
});

const Lectura = mongoose.model('Lectura', lecturaSchema);

// 2. Histórico Consolidado (Promedios por Hora)
const historicoSchema = new mongoose.Schema({
    fecha: String,         // Formato YYYY-MM-DD
    hora: String,          // Formato HH:00
    avgTemperature: Number,
    avgHumidity: Number,
    avgPressure: Number,
    avgPm25: Number,
    avgCo2: Number,
    avgMq7: Number,
    avgLdr: Number,
    avgUv: Number,
    avgAirQuality: Number,
    latitude: Number,
    longitude: Number,
    timestamp: { type: Date, default: Date.now }
});

const Historico = mongoose.model('Historico', historicoSchema);

// ==========================================
// TAREA PROGRAMADA: CONSOLIDACIÓN AUTOMÁTICA (CRON)
// ==========================================
// Ejecuta todos los días a las 23:58 PM para convertir los registros de 5 min en 24 registros por hora
async function consolidarDiaActual() {
    console.log("-> Iniciando proceso de consolidación de datos del día...");
    try {
        const inicioDia = new Date();
        inicioDia.setHours(0, 0, 0, 0);

        const finDia = new Date();
        finDia.setHours(23, 59, 59, 999);

        // Agrupación de MongoDB por hora
        const resultadoHora = await Lectura.aggregate([
            { $match: { timestamp: { $gte: inicioDia, $lte: finDia } } },
            {
                $group: {
                    _id: {
                        fecha: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } },
                        hora: { $dateToString: { format: "%H:00", date: "$timestamp" } }
                    },
                    avgTemp: { $avg: "$temperature" },
                    avgHum: { $avg: "$humidity" },
                    avgPres: { $avg: "$pressure" },
                    avgPm25: { $avg: "$pm25" },
                    avgCo2: { $avg: "$co2" },
                    avgMq7: { $avg: "$mq7" },
                    avgLdr: { $avg: "$ldr" },
                    avgUv: { $avg: "$uv" },
                    avgCalidad: { $avg: "$airQuality" },
                    lastLat: { $last: "$latitude" },
                    lastLon: { $last: "$longitude" }
                }
            },
            { $sort: { "_id.hora": 1 } }
        ]);

        for (const item of resultadoHora) {
            await Historico.updateOne(
                { fecha: item._id.fecha, hora: item._id.hora },
                {
                    $set: {
                        fecha: item._id.fecha,
                        hora: item._id.hora,
                        avgTemperature: Number(item.avgTemp.toFixed(1)),
                        avgHumidity: Number(item.avgHum.toFixed(1)),
                        avgPressure: Number(item.avgPres.toFixed(1)),
                        avgPm25: Math.round(item.avgPm25),
                        avgCo2: Math.round(item.avgCo2),
                        avgMq7: Math.round(item.avgMq7),
                        avgLdr: Math.round(item.avgLdr),
                        avgUv: Math.round(item.avgUv),
                        avgAirQuality: Math.round(item.avgCalidad),
                        latitude: item.lastLat,
                        longitude: item.lastLon,
                        timestamp: new Date(`${item._id.fecha}T${item._id.hora}:00Z`)
                    }
                },
                { upsert: true }
            );
        }

        console.log(`-> Consolidación exitosa. Se procesaron ${resultadoHora.length} bloques horarios.`);
        
        // Purgar lecturas detalladas anteriores a 48 horas para ahorrar espacio
        const dosDiasAtras = new Date(Date.now() - 48 * 60 * 60 * 1000);
        await Lectura.deleteMany({ timestamp: { $lt: dosDiasAtras } });
        console.log("-> Limpieza de registros en vivo antiguos completada.");

    } catch (err) {
        console.error("Error en la consolidación:", err);
    }
}

// Programar ejecución todos los días a las 23:58 PM
cron.schedule('58 23 * * *', () => {
    consolidarDiaActual();
});

// Endpoint manual para forzar la consolidación cuando quieras
app.post('/api/admin/consolidar', async (req, res) => {
    await consolidarDiaActual();
    res.json({ status: "success", message: "Consolidación forzada completada" });
});

// ==========================================
// ENDPOINTS REST
// ==========================================

// Guardar nueva lectura desde ESP32
app.post('/api/readings', async (req, res) => {
    try {
        const nuevaLectura = new Lectura(req.body);
        await nuevaLectura.save();
        res.status(200).json({ status: "success", message: "Lectura guardada" });
    } catch (error) {
        res.status(500).json({ status: "error", message: error.message });
    }
});

// Obtener datos del día actual (Alta Definición - cada 5 min)
app.get('/api/data/hoy', async (req, res) => {
    try {
        const inicioHoy = new Date();
        inicioHoy.setHours(0, 0, 0, 0);
        const lecturas = await Lectura.find({ timestamp: { $gte: inicioHoy } }).sort({ timestamp: 1 });
        res.json(lecturas);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Obtener la última lectura instantánea
app.get('/api/data/ultima', async (req, res) => {
    try {
        const ultima = await Lectura.findOne().sort({ timestamp: -1 });
        res.json(ultima || {});
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Obtener historial por fecha seleccionada (Baja Definición - Consolidado por Hora)
app.get('/api/data/historico', async (req, res) => {
    try {
        const { fecha } = req.query; // Ejemplo: /api/data/historico?fecha=2026-10-29
        let query = {};
        if (fecha) {
            query.fecha = fecha;
        }
        const historicos = await Historico.find(query).sort({ fecha: -1, hora: 1 }).limit(168);
        res.json(historicos);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Listar fechas disponibles en el historial
app.get('/api/data/fechas-disponibles', async (req, res) => {
    try {
        const fechas = await Historico.distinct('fecha');
        res.json(fechas.sort().reverse());
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// FRONTEND COMPLETO E INTERACTIVO
// ==========================================
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Estación Ambiental IoT - Dashboard Profesional</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --bg: #0b0f19; --card: #151c2c; --accent: #38bdf8; --text: #f8fafc; --subtext: #94a3b8; --border: #1e293b; }
        * { box-sizing: border-box; }
        body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }
        header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 15px; margin-bottom: 20px; flex-wrap: wrap; gap: 10px; }
        h1 { margin: 0; font-size: 1.5rem; color: var(--accent); }
        .tabs { display: flex; gap: 10px; }
        .tab-btn { background: var(--card); color: var(--subtext); border: 1px solid var(--border); padding: 8px 16px; border-radius: 8px; cursor: pointer; font-weight: 600; transition: 0.3s; }
        .tab-btn.active, .tab-btn:hover { background: var(--accent); color: #000; border-color: var(--accent); }
        
        /* Grid de KPIs */
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 25px; }
        .kpi-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 15px; text-align: center; }
        .kpi-card span { display: block; font-size: 0.85rem; color: var(--subtext); margin-bottom: 5px; }
        .kpi-card h2 { margin: 0; font-size: 1.6rem; color: var(--text); }

        /* Contenedores Principales */
        .view-section { display: none; }
        .view-section.active { display: block; }
        
        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; margin-bottom: 25px; }
        @media(max-width: 600px) { .charts-grid { grid-template-columns: 1fr; } }
        
        .chart-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; }
        .chart-card h3 { margin: 0 0 15px 0; font-size: 1rem; color: var(--accent); border-bottom: 1px solid var(--border); padding-bottom: 8px; }
        .chart-container { position: relative; height: 260px; width: 100%; }

        #map { height: 250px; border-radius: 12px; border: 1px solid var(--border); margin-top: 15px; }
        
        .selector-bar { display: flex; align-items: center; gap: 10px; background: var(--card); padding: 12px; border-radius: 8px; margin-bottom: 20px; border: 1px solid var(--border); }
        select { background: var(--bg); color: var(--text); border: 1px solid var(--border); padding: 6px 12px; border-radius: 6px; font-size: 0.9rem; }
    </style>
</head>
<body>

    <header>
        <div>
            <h1>Estación Ambiental IoT - Telemetría Cloud</h1>
            <small style="color: var(--subtext);">Nodo Sensor Autónomo GPS / BME280 / PMS5003 / MH-Z19C</small>
        </div>
        <div class="tabs">
            <button class="tab-btn active" onclick="switchView('vivo')">Alta Definición (Hoy - 5m)</button>
            <button class="tab-btn" onclick="switchView('historico')">Historial Consolidado (Horario)</button>
        </div>
    </header>

    <!-- METRICAS INSTANTÁNEAS -->
    <div class="kpi-grid">
        <div class="kpi-card"><span>Temperatura</span><h2 id="kpi-temp">-- °C</h2></div>
        <div class="kpi-card"><span>Humedad</span><h2 id="kpi-hum">-- %</h2></div>
        <div class="kpi-card"><span>Presión Atmosférica</span><h2 id="kpi-pres">-- hPa</h2></div>
        <div class="kpi-card"><span>Material Particulado PM2.5</span><h2 id="kpi-pm25">-- µg/m³</h2></div>
        <div class="kpi-card"><span>Dióxido de Carbono CO2</span><h2 id="kpi-co2">-- ppm</h2></div>
        <div class="kpi-card"><span>Calidad Global del Aire</span><h2 id="kpi-aqi" style="color: #4ade80;">-- %</h2></div>
    </div>

    <!-- SECCIÓN ALTA DEFINICIÓN (DÍA ACTUAL) -->
    <div id="view-vivo" class="view-section active">
        <div class="charts-grid">
            <div class="chart-card">
                <h3>Termodinámica: Temperatura y Humedad</h3>
                <div class="chart-container"><canvas id="chartTempHum"></canvas></div>
            </div>
            <div class="chart-card">
                <h3>Calidad del Aire: PM2.5 y CO2</h3>
                <div class="chart-container"><canvas id="chartAire"></canvas></div>
            </div>
            <div class="chart-card">
                <h3>Monóxido de Carbono (MQ-7) y Radiación UV</h3>
                <div class="chart-container"><canvas id="chartGasesUv"></canvas></div>
            </div>
            <div class="chart-card">
                <h3>Geolocalización GPS en Tiempo Real</h3>
                <div id="map"></div>
            </div>
        </div>
    </div>

    <!-- SECCIÓN HISTÓRICO CONSOLIDADO -->
    <div id="view-historico" class="view-section">
        <div class="selector-bar">
            <label>Seleccionar Fecha de Registro Histórico:</label>
            <select id="selectFecha" onchange="cargarHistoricoPorFecha()"></select>
            <button class="tab-btn" style="padding: 4px 10px;" onclick="forzarConsolidacion()">Consolidar Ahora</button>
        </div>

        <div class="charts-grid">
            <div class="chart-card">
                <h3>Evolución Promedio Horaria: Temperatura y Humedad</h3>
                <div class="chart-container"><canvas id="chartHistTemp"></canvas></div>
            </div>
            <div class="chart-card">
                <h3>Evolución Promedio Horaria: PM2.5 y CO2</h3>
                <div class="chart-container"><canvas id="chartHistAire"></canvas></div>
            </div>
        </div>
    </div>

    <script>
        let map, marker;
        let chartInstances = {};

        function initMap() {
            map = L.map('map').setView([-26.83, -65.20], 11); // Coordenadas por defecto Tucumán
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);
            marker = L.marker([-26.83, -65.20]).addTo(map).bindPopup("Estación IoT");
        }

        function switchView(view) {
            document.querySelectorAll('.view-section').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            
            if(view === 'vivo') {
                document.getElementById('view-vivo').classList.add('active');
                event.target.classList.add('active');
            } else {
                document.getElementById('view-historico').classList.add('active');
                event.target.classList.add('active');
                cargarFechasHistorico();
            }
        }

        function createChart(id, type, labels, datasets) {
            if (chartInstances[id]) chartInstances[id].destroy();
            const ctx = document.getElementById(id).getContext('2d');
            chartInstances[id] = new Chart(ctx, {
                type: type,
                data: { labels: labels, datasets: datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { labels: { color: '#f8fafc' } } },
                    scales: {
                        x: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' } },
                        y: { ticks: { color: '#94a3b8' }, grid: { color: '#1e293b' } }
                    }
                }
            });
        }

        async function cargarDatosAltaDefinicion() {
            const [resHoy, resUltima] = await Promise.all([
                fetch('/api/data/hoy'),
                fetch('/api/data/ultima')
            ]);
            
            const data = await resHoy.json();
            const ultima = await resUltima.json();

            // Actualizar KPIs
            if (ultima.temperature !== undefined) {
                document.getElementById('kpi-temp').innerText = ultima.temperature + ' °C';
                document.getElementById('kpi-hum').innerText = ultima.humidity + ' %';
                document.getElementById('kpi-pres').innerText = ultima.pressure + ' hPa';
                document.getElementById('kpi-pm25').innerText = ultima.pm25 + ' µg/m³';
                document.getElementById('kpi-co2').innerText = ultima.co2 + ' ppm';
                document.getElementById('kpi-aqi').innerText = ultima.airQuality + ' %';

                if(map && ultima.latitude && ultima.longitude && (ultima.latitude !== 0)) {
                    marker.setLatLng([ultima.latitude, ultima.longitude]);
                    map.setView([ultima.latitude, ultima.longitude], 14);
                }
            }

            if (data.length === 0) return;

            const labels = data.map(d => new Date(d.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}));

            // Chart 1: Temp & Hum
            createChart('chartTempHum', 'line', labels, [
                { label: 'Temperatura (°C)', data: data.map(d => d.temperature), borderColor: '#ef4444', tension: 0.3 },
                { label: 'Humedad (%)', data: data.map(d => d.humidity), borderColor: '#38bdf8', tension: 0.3 }
            ]);

            // Chart 2: PM2.5 & CO2
            createChart('chartAire', 'line', labels, [
                { label: 'PM2.5 (µg/m³)', data: data.map(d => d.pm25), borderColor: '#eab308', tension: 0.3 },
                { label: 'CO2 (ppm)', data: data.map(d => d.co2), borderColor: '#a855f7', tension: 0.3 }
            ]);

            // Chart 3: MQ-7 & UV
            createChart('chartGasesUv', 'line', labels, [
                { label: 'MQ-7 Monóxido (ADC)', data: data.map(d => d.mq7), borderColor: '#f97316', tension: 0.3 },
                { label: 'UV Sensor (ADC)', data: data.map(d => d.uv), borderColor: '#8b5cf6', tension: 0.3 }
            ]);
        }

        async function cargarFechasHistorico() {
            const res = await fetch('/api/data/fechas-disponibles');
            const fechas = await res.json();
            const select = document.getElementById('selectFecha');
            select.innerHTML = fechas.map(f => \`<option value="\${f}">\${f}</option>\`).join('');
            if(fechas.length > 0) cargarHistoricoPorFecha();
        }

        async function cargarHistoricoPorFecha() {
            const fecha = document.getElementById('selectFecha').value;
            const res = await fetch(\`/api/data/historico?fecha=\${fecha}\`);
            const data = await res.json();

            if (data.length === 0) return;

            const labels = data.map(d => d.hora);

            createChart('chartHistTemp', 'bar', labels, [
                { label: 'Temp Promedio (°C)', data: data.map(d => d.avgTemperature), backgroundColor: '#ef4444' },
                { label: 'Humedad Promedio (%)', data: data.map(d => d.avgHumidity), backgroundColor: '#38bdf8' }
            ]);

            createChart('chartHistAire', 'bar', labels, [
                { label: 'PM2.5 Promedio (µg/m³)', data: data.map(d => d.avgPm25), backgroundColor: '#eab308' },
                { label: 'CO2 Promedio (ppm)', data: data.map(d => d.avgCo2), backgroundColor: '#a855f7' }
            ]);
        }

        async function forzarConsolidacion() {
            await fetch('/api/admin/consolidar', { method: 'POST' });
            alert("Consolidación ejecutada exitosamente.");
            cargarFechasHistorico();
        }

        window.onload = () => {
            initMap();
            cargarDatosAltaDefinicion();
            setInterval(cargarDatosAltaDefinicion, 30000); // Refresco cada 30s
        };
    </script>
</body>
</html>
    `);
});

app.listen(PORT, () => {
    console.log(`-> Servidor backend profesional corriendo en puerto ${PORT}`);
});