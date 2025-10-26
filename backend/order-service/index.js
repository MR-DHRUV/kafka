import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'order-service',
    brokers: ['localhost:9094'],
});

const consumer = kafka.consumer({ groupId: 'order-service' });
const producer = kafka.producer();

const run = async () => {
    try {
        await consumer.connect();
        await producer.connect();
        console.log("Kafka connected");

        await consumer.subscribe({
            topic: 'payment-success',
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                const { userId, cart } = JSON.parse(value);
                const totalAmount = cart.reduce((acc, item) => acc + item.price, 0);

                const orderId = Math.floor(Math.random() * 1000000);
                console.log(`Order ${orderId} created for user ${userId} with total amount ${totalAmount}`);

                await producer.send({
                    topic: 'order-success',
                    messages: [
                        { value: JSON.stringify({ userId, orderId, totalAmount }) }
                    ]
                });
            }
        });

    } catch (error) {
        console.error("Error connecting Kafka Consumer:", error);
    }
}

run();