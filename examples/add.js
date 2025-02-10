import Queue from "../src/Queue.js";

(async () => {
    const QUEUE = new Queue({
        host: "127.0.0.1",
        port: 6379,
        password: "",
        db: 0
    });

    await QUEUE.init();
    console.log("✅ Conexión a Redis en Docker establecida.");

    for (let i = 1; i <= 5; i++) {
        const response = await QUEUE.add("CHANNEL", "573205104418", {
            to: "573205104418",
            message: `Hola mundo ${i}`
        });
        console.log(`response ${i}:`, response);
    }
})();