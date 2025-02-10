import Queue from "../src/Queue.js";

(async () => {
    const QUEUE = new Queue({
        host: "127.0.0.1",  // Si es local con Docker
        port: 6379,         // Puerto por defecto en Docker
        password: "",       // Redis en Docker no tiene contraseña por defecto
        db: 0               // Base de datos por defecto
    });

    console.log("✅ Conexión a Redis en Docker establecida.");

    QUEUE.process('CHANNEL', 1, (job, done) => {

    })

})();