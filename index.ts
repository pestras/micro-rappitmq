import { Micro, MicroPlugin, HealthState } from "@pestras/micro";
import { connect as mqConnect, Options, Connection, Channel, ConsumeMessage, Replies } from 'amqplib';

export type ExchangeType = "fanout" | "direct" | "topic" | "headers";

export { ConsumeMessage, Channel }

interface IQueue {
  assertOptions: Options.AssertQueue & { prefetch?: boolean },
  consumeOptions: Options.Consume,
  service: any;
  handlerName: string;
}

interface IExchange {
  assertOptions: Options.AssertExchange & { prefetch?: boolean },
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
  assertOptions?: Options.AssertQueue & { prefetch?: boolean },
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
  options?: Options.AssertExchange & { prefetch?: boolean },
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
      type: "fanout",
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
      type: "fanout",
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

  async send(content: Buffer, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertQueue(this.name, this.options);
    }

    this._channel.sendToQueue(this.name, content, options);
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

  async publish(content: Buffer, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'fanout', this.options);
    }

    this._channel.publish(this.name, '', content, options);
  }
}

export class DirectEx extends Excahnge {

  async publish(routingKey: string, content: Buffer, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'direct', this.options);
    }

    this._channel.publish(this.name, routingKey, content, options);
  }
}

export class TopicEx extends Excahnge {

  async publish(pattern: string, content: Buffer, options?: Options.Publish) {
    if (!this._channel) {
      this._channel = await connection.createChannel();
      await this._channel.assertExchange(this.name, 'topic', this.options);
    }

    this._channel.publish(this.name, pattern, content, options);
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
    try {
      connection = await mqConnect(this._connectOptions, this._socketOptions);
      Micro.logger.info("connected to rabbitmq broker successfully");
    } catch (e) {
      Micro.logger.error("error connecting to mq server", e?.message || e);
      throw e;
    }

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
    connection && connection.close();
  }

  private async _createChannel(name: string) {
    try {
      let channel = await connection.createChannel();
      return channel;
    } catch (e) {
      Micro.logger.error("error creating mq channel", e?.message || e);
      throw e;
    }
  }

  private async _createQueue(name: string, options?: Options.AssertQueue) {
    let channel = await this._createChannel(name);
    await channel.assertQueue(name, options);
    return channel;
  }

  private async _createExchange(name: string, type: ExchangeType, options?: Options.AssertExchange) {
    let channel = await this._createChannel(name);
    await channel.assertExchange(name, type, options);
    return channel;
  }

  private async _prepareQueues() {
    for (let queueName in queues) {
      let queue = queues[queueName];
      let currentService = Micro.getCurrentService(queue.service) || Micro.service;

      if (typeof currentService[queue.handlerName] !== "function") continue;

      Micro.logger.info('consuming queue: ' + queueName);

      let channel = await this._createQueue(queueName, queue.assertOptions);

      if (queue.assertOptions.prefetch)
        channel.prefetch(1);

      channel.consume(queueName, async (msg: ConsumeMessage) => {
        try {
          currentService[queue.handlerName](msg, channel);
        } catch (e) {
          Micro.logger.error(`error in queue handler!`, e?.message || e);
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

        let channel = await this._createExchange(exchangeName, 'fanout', exchange.assertOptions);
        let assertedQueue: Replies.AssertQueue;

        if (exchange.assertOptions.prefetch)
          channel.prefetch(1);

        try {
          assertedQueue = await channel.assertQueue('', { exclusive: true });
        } catch (e) {
          Micro.logger.error(`error asserting queue!`, e?.message || e);
        }
        
        exchange.routingKeys.forEach(key => channel.bindQueue(assertedQueue.queue, exchangeName, key));
        
        channel.consume(assertedQueue.queue, (msg: ConsumeMessage) => {
          try {
            currentService[exchange.handlerName](msg, channel);
          } catch (e) {
            Micro.logger.error(`error in queue handler!`, e?.message || e);
          }
        }, exchange.consumeOptions);
      }
    }
  }

  static async Request(queue: string, content: Buffer, consumeOptions?: Options.Consume & { timeout: number }) {
    if (!connection) throw "no rabbitMQ connection found!";
  
    let channel = await connection.createChannel();
    let timerId: any;
  
    let q = await channel.assertQueue('', { exclusive: true });
    let correlationId =  Math.random().toString() + Math.random().toString() + Math.random().toString();
  
    channel.sendToQueue(queue, content, { correlationId, replyTo: q.queue });
  
    return new Promise((resolve, reject) => {
      timerId = setTimeout(() => {
        channel.removeListener(q.queue, handler);
        reject(new Error("RPC request timeout!"))
      }, consumeOptions.timeout || 3000);

      function handler(msg: ConsumeMessage) {
        if (msg.properties.correlationId == correlationId) {
          clearTimeout(timerId);
          resolve(msg);
        }
      }

      channel.consume(q.queue, handler, consumeOptions);
    });
  }
}