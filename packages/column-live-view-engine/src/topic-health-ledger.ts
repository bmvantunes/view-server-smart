export type TopicHealthSubscription = {
  queuedEvents: number;
  maxQueueDepth: number;
  backpressureEvents: number;
};

type TopicHealthTotals = {
  activeSubscriptions: number;
  queuedEvents: number;
  maxQueueDepth: number;
  backpressureEvents: number;
};

type TopicHealthLedger = {
  readonly openSubscription: (subscription: object) => void;
  readonly closeSubscription: (subscription: object) => void;
  readonly updateQueueDepth: (subscription: object, queueDepth: number) => void;
  readonly markBackpressure: (subscription: object) => void;
  readonly reset: () => void;
  readonly snapshot: () => TopicHealthTotals;
};

export const createTopicHealthLedger = (): TopicHealthLedger => {
  const subscriptions = new Map<object, TopicHealthSubscription>();
  let activeSubscriptions = 0;
  let queuedEvents = 0;
  let maxQueueDepth = 0;
  let backpressureEvents = 0;

  const ensureSubscription = (subscription: object): TopicHealthSubscription | undefined =>
    subscriptions.get(subscription);

  const openSubscription = (subscription: object): void => {
    subscriptions.set(subscription, {
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
    });
    activeSubscriptions += 1;
  };

  const closeSubscription = (subscription: object): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    subscriptions.delete(subscription);
    activeSubscriptions -= 1;
    queuedEvents = Math.max(0, queuedEvents - tracked.queuedEvents);
  };

  const updateQueueDepth = (subscription: object, nextDepth: number): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    queuedEvents -= tracked.queuedEvents;
    queuedEvents += nextDepth;

    tracked.queuedEvents = nextDepth;
    if (nextDepth > tracked.maxQueueDepth) {
      tracked.maxQueueDepth = nextDepth;
      maxQueueDepth = Math.max(maxQueueDepth, nextDepth);
    }
  };

  const markBackpressure = (subscription: object): void => {
    const tracked = ensureSubscription(subscription);
    if (tracked === undefined) {
      return;
    }
    tracked.backpressureEvents += 1;
    backpressureEvents += 1;
  };

  const reset = (): void => {
    subscriptions.clear();
    activeSubscriptions = 0;
    queuedEvents = 0;
    maxQueueDepth = 0;
    backpressureEvents = 0;
  };

  const snapshot = (): TopicHealthTotals => ({
    activeSubscriptions,
    queuedEvents: Math.max(0, queuedEvents),
    maxQueueDepth,
    backpressureEvents,
  });

  return {
    openSubscription: (subscription: object): void => {
      if (subscriptions.has(subscription)) {
        return;
      }
      openSubscription(subscription);
    },
    closeSubscription: (subscription: object): void => {
      closeSubscription(subscription);
    },
    updateQueueDepth: (subscription: object, queueDepth: number): void => {
      updateQueueDepth(subscription, queueDepth);
    },
    markBackpressure: (subscription: object): void => {
      markBackpressure(subscription);
    },
    reset,
    snapshot,
  };
};
