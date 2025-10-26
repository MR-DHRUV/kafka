import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'email-service',
    brokers: ['localhost:9094'],
});

const consumer = kafka.consumer({ groupId: 'email-service' });
const producer = kafka.producer();

const run = async () => {
    try {
        await consumer.connect();
        await producer.connect();
        console.log("Kafka Consumer connected");

        await consumer.subscribe({
            topic: 'order-success',
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                const { userId, orderId, totalAmount } = JSON.parse(value);

                const emailId = Math.floor(Math.random() * 1000000);
                console.log(`Emailed user ${userId} with email id ${emailId}`);

                await producer.send({
                    topic: 'email-success',
                    messages: [
                        { value: JSON.stringify({ userId, orderId, emailId }) }
                    ]
                });
            }
        });

    } catch (error) {
        console.error("Error connecting Kafka Consumer:", error);
    }
}

run();