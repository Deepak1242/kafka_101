const {Kafka} = require('kafkajs');


const kafka = new Kafka({
    'clientId' : 'order-consumer',
    'brokers' : ['localhost:9092'],
})

const consumer = kafka.consumer({groupId: 'order-group'});

const run = async () =>{

    await consumer.connect();
    console.log('Consumer Connected');

    await consumer.subscribe({topic: 'orders', fromBeginning: true});

    await consumer.run({
        eachMessage: async({topic, partition, message}) =>{
            const order = JSON.parse(message.value.toString());
            console.log(`🎯 Consumed: ${order.item} | Offset: ${message.offset} | Partition: ${partition}`)
            await new Promise(r => setTimeout(r, 500));
        }
    })
    
  
}

run().catch(console.error);