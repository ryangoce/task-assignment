import { Redis, RedisOptions } from "ioredis";
import { EventEmitter } from "events";

export interface TaskAssignmentGroupOptions {
    createRedisClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
    initialTasks: Array<string>;
    groupId: string;
    membershipPollingTimeout?: number
}

export class TaskAssignmentGroup extends EventEmitter {
    constructor(options: TaskAssignmentGroupOptions);
    initialize(): Promise<void>;
    join(): Promise<void>;
    leave(): Promise<void>;
    addTasks(tasks: Array<string>): Promise<void>;
}