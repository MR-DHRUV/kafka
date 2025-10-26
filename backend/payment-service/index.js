import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';

const app = express();
const PORT = process.env.PORT || 8000;

const kafka = new Kafka({
    clientId: 'payment-service',
    brokers: ['localhost:9094'],
});

const producer = kafka.producer();

const connectToKafka = async () => {
    try {
        await producer.connect();
        console.log("Kafka Producer connected");
    } catch (error) {
        console.error("Error connecting Kafka Producer:", error);
    }
}

app.use(cors({
    origin: 'http://localhost:3000',
}));

app.use(express.json());

app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).send('Something broke!');
});

app.post('/payment-service', async(req, res) => {
    const { cart } = req.body;
    const userId = "12345";

    await producer.send({
        topic: 'payment-success',
        messages: [
            { value: JSON.stringify({ userId, cart }) }
        ]
    })

    return res.status(200).send("Payment processed successfully");
})

app.listen(PORT, () => {
    connectToKafka();
    console.log(`Payment service is running on port ${PORT}`);
});

