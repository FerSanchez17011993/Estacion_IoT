const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mongoose = require('mongoose');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(bodyParser.json());

// 1. Levantamos el servidor HTTP PRIMERO para garantizar respuesta inmediata en consola
app.listen(PORT, () => {
    console.log(`-> ¡Servidor corriendo con éxito en el puerto ${PORT}!`);
});

// 2. URI directa para evitar cualquier fallo de lectura del archivo .env en Windows
const MONGO_URI = "mongodb+srv://estacion_iot:ek2ZiLoeaHDZm9P0@estacion.0vujvgu.mongodb.net/estacion_iot?retryWrites=true&w=majority";

console.log("Intentando conectar a MongoDB Atlas en segundo plano...");

mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 5000
})
.then(() => {
    console.log("-> ¡Conectado exitosamente a MongoDB Atlas!");
})
.catch(err => {
    console.error("-> ADVERTENCIA: No se pudo conectar a MongoDB Atlas:", err.message);
    console.log("-> El servidor sigue funcionando en memoria localmente.");
});

// 3. Definición del Esquema y Modelo para la Base de Datos
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

// Variable temporal en memoria para consulta rápida en el panel web
let ultimaLectura = {
    temperature: 0,
    humidity: 0,
    pressure: 0,
    pm25: 0,
    co2: 0,
    mq7: 0,
    ldr: 0,
    uv: 0,
    airQuality: 100,
    latitude: 0.0,
    longitude: 0.0,
    timestamp: "Esperando datos..."
};

// Endpoint POST: Recibe los datos del ESP32, los guarda en Atlas (si hay conexión) y actualiza la memoria
app.post('/api/readings', async (req, res) => {
    try {
        const datos = req.body;
        
        ultimaLectura = {
            ...datos,
            timestamp: new Date().toLocaleString()
        };
        
        // Intentar guardar en MongoDB Atlas si está conectado
        if (mongoose.connection.readyState === 1) {
            const nuevaLecturaDB = new Lectura(datos);
            await nuevaLecturaDB.save();
            console.log("-> Datos guardados en MongoDB Atlas y memoria.");
        } else {
            console.log("-> Datos guardados solo en memoria (MongoDB no disponible aún).");
        }

        res.status(200).json({ status: "success", message: "Datos procesados correctamente" });
    } catch (error) {
        console.error("Error al procesar datos:", error);
        res.status(500).json({ status: "error", message: "Error interno del servidor" });
    }
});

// Endpoint GET: Para consultar los valores en tiempo real
app.get('/api/readings', (req, res) => {
    res.status(200).json(ultimaLectura);
});

// Vista web simple en la raíz
app.get('/', (req, res) => {
    res.send(`
        <html>
            <head>
                <title>Estacion IoT - Tiempo Real</title>
                <meta http-equiv="refresh" content="5">
                <style>
                    body { font-family: Arial, sans-serif; background: #121212; color: #e0e0e0; text-align: center; padding-top: 50px; }
                    .card { background: #1e1e1e; display: inline-block; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); text-align: left; }
                    h1 { color: #4CAF50; }
                    .val { font-weight: bold; color: #64B5F6; }
                </style>
            </head>
            <body>
                <div class="card">
                    <h1>Estacion Ambiental IoT</h1>
                    <p>Ultima actualizacion: <span class="val">${ultimaLectura.timestamp}</span></p>
                    <hr>
                    <p>Temperatura: <span class="val">${ultimaLectura.temperature} °C</span></p>
                    <p>Humedad: <span class="val">${ultimaLectura.humidity} %</span></p>
                    <p>Presion: <span class="val">${ultimaLectura.pressure} hPa</span></p>
                    <p>PM2.5: <span class="val">${ultimaLectura.pm25} ug/m3</span></p>
                    <p>CO2: <span class="val">${ultimaLectura.co2} ppm</span></p>
                    <p>Monoxido (MQ-7 Raw): <span class="val">${ultimaLectura.mq7}</span></p>
                    <p>Luz LDR: <span class="val">${ultimaLectura.ldr}</span></p>
                    <p>UV: <span class="val">${ultimaLectura.uv}</span></p>
                    <p>Calidad de Aire Total: <span class="val">${ultimaLectura.airQuality} %</span></p>
                    <p>GPS Lat/Lon: <span class="val">${ultimaLectura.latitude}, ${ultimaLectura.longitude}</span></p>
                </div>
            </body>
        </html>
    `);
});