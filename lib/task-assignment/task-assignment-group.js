const nanoid = require('nanoid').nanoid;
const _ = require('lodash');
const pubsub = require('../pubsub');
const EventEmitter = require('events');

class TaskAssignmentGroup extends EventEmitter {
    /**
     * 
     * @param {import('./task-assignment-group').TaskAssignmentGroupOptions} options 
     */
    constructor(options) {
        super(options);
        options = options || {};
        var defaults = {
            membershipPollingTimeout: 10000
        };

        this._options = _.defaults(options, defaults);

        if (!this._options.createRedisClient) {
            throw new Error('createRedisClient is required');
        }

        if (!this._options.initialTasks || !Array.isArray(this._options.initialTasks) || this._options.initialTasks.length == 0) {
            throw new Error('initialTasks must be an array and should not be empty');
        }

        if (!this._options.groupId) {
            throw new Error('groupId is required');
        }

        this._tasks = _.clone(this._options.initialTasks);

        this._pubsubClient = pubsub.createPubSubClient({
            createRedisClient: this._options.createRedisClient
        });

        this._redisClient = this._options.createRedisClient('client');


        this._redisClient.defineCommand('sanitizemembers', {
            numberOfKeys: 1,
            lua: `
          local members_set = KEYS[1]
          local expiration_time = ARGV[1]

          redis.call('ZREMRANGEBYSCORE', members_set, '-inf', expiration_time)
          local active_members = redis.call('ZRANGEBYSCORE', members_set, expiration_time, '+inf');
          return active_members
          `
        });


        this._isMember = false;
        this._isInitialized = false;
    }

    async initialize() {
        this._doLeaderJob();

        this._isInitialized = true;
        // return void;
    }

    async join() {
        if (!this._isInitialized) {
            throw new Error('Group not yet initialized. Call initialize first');
        }

        if (this._isMember) {
            throw new Error('You cannot join when you are already a member');
        }

        // add me as a member in our group membership set
        const membershipId = nanoid();
        this._isMember = true;
        this._doMemberJob(membershipId);
    }

    async leave() {
        if (!this._isMember) {
            throw new Error('You cannot leave if you are not a member');
        }
        this._isMember = false;
    }

    /**
     * 
     * @param {Array<string>} tasks 
     */
    async addTasks(tasks) {
        if (!this._isInitialized) {
            throw new Error('Group not yet initialized. Call initialize first');
        }
        // return void
    }

    _getMembersKey() {
        return `task-assignment-groups:${this._options.groupId}:members`;
    }

    _getLeaderLockKey() {
        return `task-assignment-groups:${this._options.groupId}:leader-lock`;
    }

    _getMembersStreamId() {
        return `task-assignment-groups:${this._options.groupId}:members-stream`;
    }


    /**
     * 
     * @param {string} membershipId 
     */
    async _doMemberJob(membershipId) {
        // register to the control stream
        const self = this;
        const membersStreamId = this._getMembersStreamId();
        const subscriptionToken = this._pubsubClient.subscribe(membersStreamId, function(error, message) {
            const msg = JSON.parse(message);
            if (msg.type == 'rebalance') {
                const assignedTasks = msg.payload[membershipId];
                if (assignedTasks) {
                    self.emit('rebalance', assignedTasks);
                }
            }
        });

        while (this._isMember) {
            const taskAssignmentGroupKey = this._getMembersKey();
            await this._redisClient.zadd(taskAssignmentGroupKey, Date.now(), membershipId);
            await this._sleep(this._options.membershipPollingTimeout);
        }

        await this._pubsubClient.unsubscribe(subscriptionToken);
    }

    async _doLeaderJob() {
        const lockDefaults = {
            driftFactor: 0.01, // time in ms
            retryCount: 10,
            retryDelay: 200, // time in ms
            retryJitter: 200 // time in ms
        }

        const RedisLock = require('redlock');
        const redLock = new RedisLock([this._redisClient], lockDefaults);
        const ttlDuration = this._options.membershipPollingTimeout;

        try {
            const lockKey = this._getLeaderLockKey();
            let lock = await redLock.lock(lockKey, ttlDuration);

            console.log('lock acquired. doing task assignment group leader job');
            // acquired a lock. do the job
            let continueJob = true;

            let lastActiveMembers = [];
            while (continueJob) {
                try {

                    const maxMemberActiveTime = Date.now() - (this._options.membershipPollingTimeout * 1.5);

                    /**
                     * @type{Array<string>}
                     */
                    const activeMembers = await this._redisClient.sanitizemembers(this._getMembersKey(), maxMemberActiveTime);
                    const sortedActiveMembers = activeMembers.sort();

                    if (!_.isEqual(lastActiveMembers, sortedActiveMembers)) {
                        // rebalance
                        const membersStreamId = this._getMembersStreamId();

                        const newAssignment = {};

                        // iterate and assign
                        for (let i = 0; i < this._tasks.length; i++) {
                            const taskId = this._tasks[i];
                        
                            const member = sortedActiveMembers[i % sortedActiveMembers.length];

                            if (!newAssignment[member]) {
                                newAssignment[member] = [];
                            }

                            newAssignment[member].push(taskId);
                        }

                        console.log('got new assignment. rebalancing', newAssignment);

                        const message = {
                            type: 'rebalance',
                            payload: newAssignment
                        }

                        await this._pubsubClient.publish(membersStreamId, JSON.stringify(message))
                    }

                    lastActiveMembers = sortedActiveMembers;

                    await lock.extend(ttlDuration);
                    await this._sleep(ttlDuration / 2);
                } catch (error) {
                    console.error('error in while loop in shared job', error);
                    // if there is an error then exit while loop and then contend again
                    continueJob = false;
                }
            }
        } catch (error) {
            if (error.name == 'LockError') {
                // ignore this just try again later to acquire lock
                // debug('lost in lock contention. will try again in', ttlDuration);
            } else {
                console.error('error in doing shared timer job', error, error.name);
            }
        } finally {
            // sleep before contending again
            await this._sleep(ttlDuration + (Math.floor(Math.random() * (ttlDuration / 2))));
            await this._doLeaderJob();
        }
    }

    async _sleep(timeout) {
        return new Promise((resolve) => {
            const timeoutRef = setTimeout(() => {
                clearTimeout(timeoutRef);
                resolve();
            }, timeout);
        })
    }
}

module.exports.TaskAssignmentGroup = TaskAssignmentGroup;