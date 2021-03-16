# Pestras Micro RabbitMQ

Pestras microservice plugin for rabbitmq broker support.

## install

```bash
npm i @pestras/micro @pestras/micro-rabbitmq
```

## Plug In

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {}

Micro.start(Test);
```

**MicroMQ** class accepts a two arguments.

Name            | Type     | Default         | Description
----            | -----    | ------          | -----
connectOptions  | string \| Options.Connect | required | see [RabbitMQ Docs](https://www.rabbitmq.com/tutorials/tutorial-one-javascript.html)
socketConnection | any | null | see [RabbitMQ Docs](https://www.rabbitmq.com/tutorials/tutorial-one-javascript.html)

# Decorators:

**MicroMQ** provides several decorators to organize our code.

## QUEUE:

As the name suggests, it helps to consume queue messages.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, QUEUE, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  @QUEUE("hello", { durable: false }, { noAck: false })
  handler(msg: ConsumeMessage, channel: Channel) {
    console.log(msg.content.toString());
  }
}

Micro.start(Test);
```

**QUEUE** decorator accepts two arguments, name of the queue and the optional **AssertQueue** options.

## FANOUT:

Helps to consume published messages as in **Publish/Subscribe** pattern.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, FANOUT, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  FANOUT("hello", { durable: false }, { noAck: false })
  handler(msg: ConsumeMessage, channel: Channel) {
    console.log(msg.content.toString());
  }
}

Micro.start(Test);
```

## DIRECT:

Helps to consume published messages with specific routing keys.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, DIRECT, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  DIRECT("logs", ["error", "wran"], { durable: false }, { noAck: false })
  handler(msg: ConsumeMessage, channel: Channel) {
    console.log(msg.content.toString());
  }
}

Micro.start(Test);
```

## TOPIC:

Helps to consume published messages with specific patterns.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, TOPIC, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  TOPIC("logs", ["kern.*", "*.critical"], { durable: false }, { noAck: false })
  handler(msg: ConsumeMessage, channel: Channel) {
    console.log(msg.content.toString());
  }
}

Micro.start(Test);
```

# Producing Messages:

We can produce messages using the channel instance in the second argument provided by messages handlers methods.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, QUEUE, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  QUEUE("hello", { durable: false })
  handler(msg: ConsumeMessage, channel: Channel) {
    console.log(msg.content.toString());

    // producing messages using same queue channel
    channel.sendToQueue(queueName, content);
  }
}

Micro.start(Test);
```

However what if we want to produce meesages without having to do any messaging consuming.

**MicroMQ** provides several ways to produce messages as follows:

## Sent To A Queue:

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, Queue } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  async someMethod() {
    // second argument is optional
    let queue = new Queue("hello", { durable: false });

    // second argument is optional
    await queue.send(Buffer.from("Hello World!"), { expiration: 60 * 1000 });
    // or
    await queue.channel.sendToQueue("hello", Buffer.from("Hello World!"), { expiration: 60 * 1000 });
  }
}

Micro.start(Test);
```

## Publish fanout:

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, FanoutEx } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  async someMethod() {
    // second argument is optional
    let fanout = new FanoutEx("hello", { durable: false });

    // second argument is optional
    await fanout.publish(Buffer.from("Hello World!"), { expiration: 60 * 1000 });
    // or
    await fanout.channel.publish("hello", '', Buffer.from("Hello World!"), { expiration: 60 * 1000 });
  }
}

Micro.start(Test);
```

## Publish direct:

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, DirectEx } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  async someMethod() {
    // second argument is optional
    let direct = new DirectEx("hello", { durable: false });

    // third argument is optional
    await direct.publish(Buffer.from("Hello World!"), "greetings", { expiration: 60 * 1000 });
    // or
    await direct.channel.publish("hello", 'greetings', Buffer.from("Hello World!"), { expiration: 60 * 1000 });
  }
}

Micro.start(Test);
```

## Publish topic:

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, TopicEx } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  async someMethod() {
    // second argument is optional
    let topic = new TopicEx("hello", { durable: false });

    // third argument is optional
    await topic.publish(Buffer.from("Hello World!"), "greetings.all", { expiration: 60 * 1000 });
    // or
    await topic.channel.publish("hello", 'greetings.all', Buffer.from("Hello World!"), { expiration: 60 * 1000 });
  }
}

Micro.start(Test);
```

# RPC

**RabbitMQ** has support for **Request/Reply** pattern, and we can achiev that in our service.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class Test {

  async getUserInfo(token: string) {
    try {
      // timeout default to 30000.
      let msg = await MicroMQ.Request("auth", Buffer.from(token), { timeout: 10000, noAck: true });
      console.log(msg.content.toString());
    } catch (e) {
      console.error(e.message);
    }
  }
}

Micro.start(Test);
```

We can make the reply in any **QUEUE** handler. 

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, QUEUE, ConsumeMessage, Channel } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class AuthService {

  QUEUE("auth", { durable: true })
  handler(msg: ConsumeMessage, channel: Channel) {
    let token = msg.content.toString();
    let user: any;
    // fetch user somehow

    channel.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringfy(user)), { correlationId: msg.properties.correlationId });
  }
}

Micro.start(AuthService);
```

# MicroMQ Events

**MicroMQ** provides a single event triggered when a connection to rabbitmq borker made successfully.

```ts
import { SERVICE, Micro } from '@pestras/micro';
import { MicroMQ, MicroMQEvent } from '@pestras/micro-rabbitmq';

Micro.plugin(new MicroMQ(connectOptions, socketOptions));

@SERVICE()
class AuthService implements MicroMQEvent {

  onConnection() {

  }
  
}

Micro.start(AuthService);
```

# SubServices:

**MicroMQ** supports *pestras/micro* subservice, so we can distribute our consumers decoraters into them as well.

Also **onConnection** event will be triggered when implemented in any subservice.