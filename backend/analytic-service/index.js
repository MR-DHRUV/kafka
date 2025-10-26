import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'analytic-service',
    brokers: ['localhost:9094'],
});

const consumer = kafka.consumer({ groupId: 'analytic-service' });

const onMessage = async({ topic, partition, message }) => {
    switch(topic) {
        case 'payment-success': {
            const value = message.value.toString();
            const { userId, cart } = JSON.parse(value);

            const totalAmount = cart.reduce((acc, item) => acc + item.price, 0);

            console.log(`Received message: User ID: ${userId}, Cart: ${JSON.stringify(cart)}`);
            console.log(`Total Amount: ${totalAmount}`);
            break;
        }
        case 'order-success': {
            const value = message.value.toString();
            const { userId, orderId, totalAmount } = JSON.parse(value);

            console.log(`Received message: User ID: ${userId}, Order ID: ${orderId}`);
            console.log(`Total Amount: ${totalAmount}`);
            break;
        }
        case 'email-success': {
            const value = message.value.toString();
            const { userId, orderId, emailId } = JSON.parse(value);

            console.log(`Received message: User ID: ${userId}, Order ID: ${orderId}, Email ID: ${emailId}`);
            break;
        }
    }
}

const run = async () => {
    try {
        await consumer.connect();
        console.log("Kafka Consumer connected");

        await consumer.subscribe({
            topics: ['payment-success', 'order-success', 'email-success'],
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: onMessage
        });

    } catch (error) {
        console.error("Error connecting Kafka Consumer:", error);        
    }
}

run();