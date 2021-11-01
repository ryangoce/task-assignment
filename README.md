# Introduction

This is an implementation of assigning long running tasks to a pool of workers. 

You can view this like a Kafka partition leader assignment but in this case the partition leader is the task itself.

# Usage

```javascript
const TaskAssignmentGroup = require('./index').TaskAssignmentGroup;
const Redis = require('ioredis');
const redisClient = new Redis(6379, 'localhost'); 

const taskGroup = new TaskAssignmentGroup({
    initialTasks: ['feed_pet', 'buy_groceries'],
    createRedisClient: function (type) {
        switch (type) {
            case 'client':
                return redisClient;

            case 'subscriber':
                return new Redis(6379, 'localhost');

            default:
                throw new Error('not known type');
        }
    },
    groupId: 'ryan_home',
    membershipPollingTimeout: 10000
});

await taskGroup.initialize();
await taskGroup.join(); 

taskGroup.on('rebalance', async (updatedAssignments) => {
    console.log('got taskGroup assignments', updatedAssignments);
    // do the assignment
});

// simulate another client and join the same group
const taskGroup2 = new TaskAssignmentGroup({
    initialTasks: ['feed_pet', 'buy_groceries'],
    createRedisClient: function (type) {
        switch (type) {
            case 'client':
                return redisClient;

            case 'subscriber':
                return new Redis(6379, 'localhost');

            default:
                throw new Error('not known type');
        }
    },
    groupId: 'ryan_home',
    membershipPollingTimeout: 10000
});

await taskGroup2.initialize();
await taskGroup2.join(); 

taskGroup2.on('rebalance', async (updatedAssignments) => {
    console.log('got taskGroup2 assignments', updatedAssignments);
    // do the assignment
});

```