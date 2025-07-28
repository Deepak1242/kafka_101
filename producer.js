const {v4 : uuidv4} = require('uuid');
const { Kafka} = require('kafkajs');



const kafka = new Kafka({
    clientId: 'order-producer',
    brokers: ['localhost:9092'],
})


const producer = kafka.producer();


const run = async () =>{

    await producer.connect();
    console.log('Producer Connected')
    for (let i=1 ; i<=10; i++){

        const order ={

            id:uuidv4(),
            amount: Math.floor(Math.random() * 1000),
            currency: 'USD',
            item: `Laptop ${i}`,
            timestamp: new Date().toISOString(),

        }
        await producer.send({
            topic: 'orders',
            messages:[
                {   key: order.id,
                    value: JSON.stringify(order),
                    partition: i % 3
                }
            ]
        });
        console.log(`Produced order: ${order}`);
        await new Promise(r=> setTimeout(r, 1000));



    }

    await producer.disconnect();
    console.log('Producer disconnected');



}

run().catch(console.error);