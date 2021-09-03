import { Micro, MicroPlugin, HealthState } from "@pestras/micro";
import { connect as mqConnect, Options, Connection, Channel, ConsumeMessage, Replies, ConsumeMessageFields, MessageProperties } from 'amqplib';

export type ExchangeType = "fanout" | "direct" | "topic" | "headers";

export { ConsumeMessage, Channel }

export class MQMsg<T = any> {
  fields: ConsumeMessageFields;
  properties: MessageProperties;
  content: Buffer;
  json: T;

  constructor(private _msg: ConsumeMessage, public channel?: Channel) {
    this.fields = this._msg.fields;
    this.properties = this._msg.properties;
    this.content = this._msg.content;
    this.json = JSON.parse(this._msg.content.toString());
  }

  respond(content: any) {
    if (this.channel)
      this.channel.sendToQueue(
        this.properties.replyTo,
        content !== undefined ? Buffer.from(JSON.stringify(content)) : content,
        { correlationId: this.properties.correlationId }
      );
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

let connection: Connection;
const queues: { [key: string]: IQueue } = {};
const exchanges: { [key: string]: IExchange[] } = {};

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

export class Queue {
  protected _channel: Channel;
  constructor(
    public readonly name: string,
    public readonly options?: Options.AssertQueue) {
  }

  get channel() { return this._channel; }

  async send(content: any, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertQueue(this.name, this.options);
    }

    this._channel.sendToQueue(this.name, content !== undefined ? Buffer.from(JSON.stringify(content)) : content, options);
  }
}

export abstract class Excahnge {
  protected _channel: Channel;

  constructor(
    public readonly name: string,
    public readonly options?: Options.AssertExchange) {
  }

  get channel() { return this._channel; }

  abstract publish(...args: any[]): Promise<void>;
}

export class FanoutEx extends Excahnge {

  async publish(content: any, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'fanout', this.options);
    }

    this._channel.publish(this.name, '', content !== undefined ? Buffer.from(JSON.stringify(content)) : content, options);
  }
}

export class DirectEx extends Excahnge {

  async publish(routingKey: string, content: any, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'direct', this.options);
    }

    this._channel.publish(this.name, routingKey, content !== undefined ? Buffer.from(JSON.stringify(content)) : content, options);
  }
}

export class TopicEx extends Excahnge {

  async publish(pattern: string, content: any, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'topic', this.options);
    }

    this._channel.publish(this.name, pattern, content !== undefined ? Buffer.from(JSON.stringify(content)) : content, options);
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

  private async _createChannel() {
    try {
      let channel = await connection.createChannel();
      return channel;
    } catch (e) {
      Micro.logger.error(e, "error creating mq channel");
    }
  }

  private async _createQueue(name: string, options?: Options.AssertQueue) {
    let channel = await this._createChannel();
    await channel.assertQueue(name, options);
    return channel;
  }

  private async _createExchange(name: string, type: ExchangeType, options?: Options.AssertExchange) {
    let channel = await this._createChannel();
    await channel.assertExchange(name, type, options);
    return channel;
  }

  private async _prepareQueues() {
    for (let queueName in queues) {
      let queue = queues[queueName];
      let currentService = Micro.getCurrentService(queue.service) || Micro.service;

      if (typeof currentService[queue.handlerName] !== "function")
        continue;

      Micro.logger.info('consuming queue: ' + queueName);

      let channel = await this._createQueue(queueName, queue.assertOptions);

      if (queue.assertOptions.prefetch)
        channel.prefetch(Math.abs(queue.assertOptions.prefetch));

      channel.consume(queueName, async (msg: ConsumeMessage) => {
        try {
          currentService[queue.handlerName](new MQMsg(msg, channel));
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

        let channel = await this._createExchange(exchangeName, exchange.type, exchange.assertOptions);
        let assertedQueue: Replies.AssertQueue;

        if (exchange.assertOptions.prefetch)
          channel.prefetch(Math.abs(exchange.assertOptions.prefetch));

        try {
          assertedQueue = await channel.assertQueue('', { exclusive: true });
        } catch (e) {
          Micro.logger.error(e, `error asserting queue!`);
        }

        exchange.routingKeys.forEach(key => channel.bindQueue(assertedQueue.queue, exchangeName, key));

        channel.consume(assertedQueue.queue, (msg: ConsumeMessage) => {
          try {
            currentService[exchange.handlerName](new MQMsg(msg, channel));
          } catch (e) {
            Micro.logger.error(e, `error in queue handler: ${exchange.handlerName}`);
          }
        }, exchange.consumeOptions);
      }
    }
  }

  static async Request<T = any>(queue: string, content: any, consumeOptions?: Options.Consume & { timeout?: number }): Promise<MQMsg<T>> {
    return new Promise(async (resolve, reject) => {
      if (!connection) reject("no rabbitMQ connection found!");

      let channel = await connection.createChannel();
      let timerId: any;

      let q = await channel.assertQueue('', { exclusive: true });
      let correlationId = Math.random().toString() + Math.random().toString() + Math.random().toString();
      let buffer = content != undefined ? Buffer.from(JSON.stringify(content)) : undefined;

      channel.sendToQueue(queue, buffer, { correlationId, replyTo: q.queue });

      timerId = setTimeout(() => {
        channel.removeListener(q.queue, handler);
        reject(new Error("RPC request timeout!"))
      }, consumeOptions?.timeout || 3000);

      function handler(msg: ConsumeMessage) {
        if (msg.properties.correlationId == correlationId) {
          clearTimeout(timerId);
          resolve(new MQMsg<T>(msg));
        }
      }

      channel.consume(q.queue, handler, consumeOptions);
    });
  }

  private static queues: { [key: string]: Queue } = {};
  private static fanouts: { [key: string]: FanoutEx } = {};
  private static directs: { [key: string]: Excahnge } = {};
  private static topics: { [key: string]: Excahnge } = {};  

  static Queue(name: string, options?: Options.AssertQueue) {
    if (MicroMQ.queues[name])
      return MicroMQ.queues[name];

    return MicroMQ.queues[name] = new Queue(name, options);
  }

  static Fanout(name: string, options?: Options.AssertExchange): FanoutEx {
    if (MicroMQ.fanouts[name])
      return MicroMQ.fanouts[name];

    return MicroMQ.fanouts[name] = new FanoutEx(name, options);
  }

  static Direct(name: string, options?: Options.AssertExchange): DirectEx {
    if (MicroMQ.directs[name])
      return MicroMQ.directs[name];

    return MicroMQ.directs[name] = new DirectEx(name, options);
  }

  static Topic(name: string, options?: Options.AssertExchange): TopicEx {
    if (MicroMQ.topics[name])
      return MicroMQ.topics[name];

    return MicroMQ.topics[name] = new TopicEx(name, options);
  }
}