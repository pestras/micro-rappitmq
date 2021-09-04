import { Micro, MicroPlugin, HealthState } from "@pestras/micro";
import { connect as mqConnect, Options, Connection, Channel, ConsumeMessage, Replies, ConsumeMessageFields, MessageProperties } from 'amqplib';

export type ExchangeType = "fanout" | "direct" | "topic" | "headers";

export * from 'amqplib';

let connection: Connection;
let publisher: Channel;
let consumer: Channel;

const queues: { [key: string]: IQueue } = {};
const exchanges: { [key: string]: IExchange[] } = {};

export class MQMsg<T = any> {
  fields: ConsumeMessageFields;
  properties: MessageProperties;
  content: Buffer;
  json: T;

  constructor(private _msg: ConsumeMessage, public channel?: Channel) {
    this.fields = this._msg.fields;
    this.properties = this._msg.properties;
    this.content = this._msg.content;
    this.json = !!this._msg.content.toString() ? JSON.parse(this._msg.content.toString()) : this._msg.content.toString();
  }

  respond(content: any, acknowledge = false) {
    if (this.channel) {
      this.channel.sendToQueue(
        this.properties.replyTo,
        content !== undefined ? Buffer.from(JSON.stringify(content)) : content,
        { correlationId: this.properties.correlationId }
      );

      acknowledge && this.channel.ack(this._msg);
    }
  }

  acknowledge() {
    if (this.channel)
      this.channel.ack(this._msg);
  }
}

interface IQueue {
  assertOptions: Options.AssertQueue & { prefetch?: number },
  consumeOptions: Options.Consume,
  service: any;
  handlerName: string;
}

interface IExchange {
  assertOptions: Options.AssertExchange & { prefetch?: number },
  consumeOptions: Options.Consume,
  routingKeys?: string[];
  type: ExchangeType
  service: any;
  handlerName: string;
}

export function QUEUE(
  name: string,
  assertOptions?: Options.AssertQueue & { prefetch?: number },
  consumeOptions?: Options.Consume) {

  return (target: any, key: string) => {
    queues[name] = {
      handlerName: key,
      service: target.constructor,
      assertOptions: assertOptions || {},
      consumeOptions
    };
  }
}

export function FANOUT(
  name: string,
  options?: Options.AssertExchange & { prefetch?: number },
  consumeOptions?: Options.Consume) {
  return (target: any, key: string) => {

    exchanges[name] = exchanges[name] || [];
    exchanges[name].push({
      type: "fanout",
      handlerName: key,
      routingKeys: [''],
      service: target.constructor,
      assertOptions: options || {},
      consumeOptions
    });
  }
}

export function DIRECT(
  name: string,
  routingKeys: string[],
  options?: Options.AssertExchange,
  consumeOptions?: Options.Consume) {

  return (target: any, key: string) => {
    exchanges[name] = exchanges[name] || [];
    exchanges[name].push({
      type: "direct",
      handlerName: key,
      routingKeys: routingKeys,
      service: target.constructor,
      assertOptions: options || {},
      consumeOptions
    });
  }
}

export function TOPIC(
  name: string,
  patterns: string[],
  options?: Options.AssertExchange,
  consumeOptions?: Options.Consume) {

  return (target: any, key: string) => {
    exchanges[name] = exchanges[name] || [];
    exchanges[name].push({
      type: "topic",
      handlerName: key,
      routingKeys: patterns,
      service: target.constructor,
      assertOptions: options || {},
      consumeOptions
    });
  }
}

export interface MicroMQEvents {
  onConnection(): void;
}

export class MicroMQ extends MicroPlugin implements HealthState {
  healthy = false;
  ready = false;

  constructor(
    private _connectOptions: string | Options.Connect,
    private _socketOptions?: any) {
    super();
  }

  async init() {
    connection = await mqConnect(this._connectOptions, this._socketOptions);
    Micro.logger.info("connected to rabbitmq broker successfully");

    publisher = await connection.createChannel();
    consumer = await connection.createChannel();

    await this._prepareQueues();
    await this._prepareExchanges();

    if (typeof Micro.service.onConnection === "function")
      Micro.service.onConnection();

    for (let service of Micro.subServices)
      if (typeof service.onConnection === "function")
        service.onConnection();

    this.healthy = true;
    this.ready = true;

  }

  onExit() {
    Micro.logger.warn("closing rabbitmq connection");
    connection && connection.close();
    Micro.logger.warn("Rabbitmq connection closed");
  }

  private async _prepareQueues() {
    for (let queueName in queues) {
      let queue = queues[queueName];
      let currentService = Micro.getCurrentService(queue.service) || Micro.service;

      if (typeof currentService[queue.handlerName] !== "function")
        continue;

      Micro.logger.info('consuming queue: ' + queueName);

      await consumer.assertQueue(queueName, queue.assertOptions);

      if (queue.assertOptions.prefetch)
        consumer.prefetch(Math.abs(queue.assertOptions.prefetch));

      consumer.consume(queueName, async (msg: ConsumeMessage) => {
        try {
          currentService[queue.handlerName](new MQMsg(msg, consumer));
        } catch (e) {
          Micro.logger.error(e, `error in queue handler: ${queue.handlerName}`);
        }
      }, queue.consumeOptions);
    }
  }

  private async _prepareExchanges() {
    for (let exchangeName in exchanges) {
      for (let exchange of exchanges[exchangeName]) {
        let currentService = Micro.getCurrentService(exchange.service) || Micro.service;

        if (typeof currentService[exchange.handlerName] !== "function") continue;

        Micro.logger.info('consuming exchange: ' + exchangeName);

        await consumer.assertExchange(exchangeName, exchange.type, exchange.assertOptions);
        let assertedQueue: Replies.AssertQueue;

        try {
          assertedQueue = await consumer.assertQueue('', { exclusive: true });
        } catch (e) {
          Micro.logger.error(e, `error asserting queue!`);
        }

        exchange.routingKeys.forEach(key => consumer.bindQueue(assertedQueue.queue, exchangeName, key));

        consumer.consume(assertedQueue.queue, (msg: ConsumeMessage) => {
          try {
            currentService[exchange.handlerName](new MQMsg(msg, consumer));
          } catch (e) {
            Micro.logger.error(e, `error in queue handler: ${exchange.handlerName}`);
          }
        }, exchange.consumeOptions);
      }
    }
  }

  static async Request<T = any>(queue: string, content: any, timeout = 3000): Promise<MQMsg<T>> {
    return new Promise(async (resolve, reject) => {
      if (!connection) reject("no rabbitMQ connection found!");

      let channel = await connection.createChannel();
      let timerId: any;

      let q = await channel.assertQueue('', { exclusive: true, autoDelete: true, expires: timeout, durable: false });
      let correlationId = Math.random().toString() + Math.random().toString() + Math.random().toString();
      let buffer = content != undefined ? Buffer.from(JSON.stringify(content)) : undefined;

      publisher.sendToQueue(queue, buffer, { correlationId, replyTo: q.queue });

      timerId = setTimeout(() => {
        channel.removeAllListeners();
        channel.deleteQueue(q.queue, { ifEmpty: false, ifUnused: false });
        reject(new Error("RPC request timeout!"))
      }, timeout);

      function handler(msg: ConsumeMessage) {
        if (msg.properties.correlationId == correlationId) {
          channel.removeAllListeners();
          channel.deleteQueue(q.queue, { ifEmpty: false, ifUnused: false });
          clearTimeout(timerId);
          resolve(new MQMsg<T>(msg));
        }

      }

      channel.consume(q.queue, handler, { noAck: true });
    });
  }  

  static async Queue(name: string, content?: any, options?: Options.AssertQueue) {
    await publisher.assertQueue(name, options);
    return publisher.sendToQueue(name, content !== undefined ? Buffer.from(JSON.stringify(content)) : content);
  }

  static async Fanout(name: string, content?: any, options?: Options.AssertExchange) {    
    await publisher.assertExchange(name, 'fanout', options);
    return publisher.publish(name, '', content !== undefined ? Buffer.from(JSON.stringify(content)) : content);
  }

  static async Direct(name: string, routingKey: string, content?: any, options?: Options.AssertExchange) {    
    await publisher.assertExchange(name, 'direct', options);
    return publisher.publish(name, routingKey, content !== undefined ? Buffer.from(JSON.stringify(content)) : content);
  }

  static async Topic(name: string, pattern: string, content?: any, options?: Options.AssertExchange) {    
    await publisher.assertExchange(name, 'topic', options);
    return publisher.publish(name, pattern, content !== undefined ? Buffer.from(JSON.stringify(content)) : content);
  }
}