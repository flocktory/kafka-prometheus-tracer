(ns com.flocktory.component.kafka-prometheus-tracer
  (:require [com.stuartsierra.component :as component]
            [com.flocktory.protocol.tracer :as tracer])
  (:import (io.prometheus.client Summary Summary$Builder Gauge Counter Summary$Child Counter$Child Gauge$Child CollectorRegistry)))

(defn- elapsed-time
  [storage context]
  (- (System/nanoTime) (get @storage context)))

(defn- register-summary
  [name label-names help]
  (-> (Summary/build)
      (.name name)
      ^Summary$Builder (.labelNames label-names)
      (.maxAgeSeconds (long 60))
      (.ageBuckets (int 3))
      (.help help)
      (.register)))

(defn- register-gauge
  [name label-names help]
  (-> (Gauge/build)
      (.name name)
      (.labelNames label-names)
      (.help help)
      (.register)))

(defn- register-counter
  [name label-names help]
  (-> (Counter/build)
      (.name name)
      (.labelNames label-names)
      (.help help)
      (.register)))

(defn- get-prometheus
  [this key]
  (get-in this [::prometheus key]))

(defn- observe-summary
  [this summary-key context value]
  (-> (get-prometheus this summary-key)
      ^Summary$Child (.labels (into-array context))
      (.observe value)))

(defn- inc-counter
  [this counter-key context]
  (-> (get-prometheus this counter-key)
      ^Counter$Child (.labels (into-array context))
      (.inc)))

(defn- set-gauge
  [this gauge-key context value]
  (-> (get-prometheus this gauge-key)
      ^Gauge$Child (.labels (into-array context))
      (.set value)))

(defn- inc-gauge
  [this gauge-key context]
  (-> (get-prometheus this gauge-key)
      ^Gauge$Child (.labels (into-array context))
      (.inc)))

(defn- dec-gauge
  [this gauge-key context]
  (-> (get-prometheus this gauge-key)
      ^Gauge$Child (.labels (into-array context))
      (.dec)))

(defn- get-storage
  [this key]
  (get-in this [::storage key]))

(defn- by-group-id
  [group-id]
  [group-id])

(defn- by-topic-partition
  [group-id {:keys [topic partition]}]
  [group-id topic (str partition)])

(defn- write-poll-interval!
  [this context]
  (let [storage (get-storage this ::poll-request)
        last-poll-time (get @storage context)]
    (when last-poll-time
      (observe-summary this ::poll-interval-summary context (elapsed-time storage context)))))

(defrecord prometheus-tracer []
  tracer/ITracerName
  (tracer-name [this] "prometheus-tracer")

  tracer/IOnConsumerFail
  (on-consumer-fail
    [this group-id exception]
    (let [context (by-group-id group-id)]
      ;; in case of exception write last poll-interval
      (write-poll-interval! this context)
      (inc-counter this :failed-consumer-counter context)))

  tracer/IBeforePoll
  (before-poll
    [this group-id]
    (let [context (by-group-id group-id)]
      (write-poll-interval! this context)
      (swap! (get-storage this ::poll-request) assoc context (System/nanoTime))))

  tracer/IAfterPoll
  (after-poll
    [this group-id records-count]
    (let [context (by-group-id group-id)
          storage (get-storage this ::poll-request)]
      (observe-summary this ::poll-request-summary context (elapsed-time storage context))
      (observe-summary this ::poll-records-count-summary context records-count)))

  tracer/IBeforeConsume
  (before-consume
    [this group-id records-count]
    (let [context (by-group-id group-id)]
      (-> (get-storage this ::consume)
          (swap! assoc context (System/nanoTime)))))

  tracer/IAfterConsume
  (after-consume
    [this group-id records-count]
    (let [context (by-group-id group-id)
          storage (get-storage this ::consume)]
      (observe-summary this ::consume-summary context (elapsed-time storage context))))

  tracer/IOnConsumeError
  (on-consume-error
    [this group-id records-count exception]
    (let [context (by-group-id group-id)
          storage (get-storage this ::consume)]
      (observe-summary this ::consume-summary context (elapsed-time storage context))))

  tracer/IBeforeConsumeRecord
  (before-consume-record
    [this group-id record]
    (let [context (by-topic-partition group-id record)]
      (-> (get-storage this ::consume-record)
          (swap! assoc context (System/nanoTime)))))

  tracer/IAfterConsumeRecord
  (after-consume-record
    [this group-id record]
    (let [context (by-topic-partition group-id record)
          storage (get-storage this ::consume-record)]
      (observe-summary this ::consume-record-summary context (elapsed-time storage context))))

  tracer/IOnConsumeRecordError
  (on-consume-record-error
    [this group-id record exception]
    (let [context (by-topic-partition group-id record)
          storage (get-storage this ::consume-record)]
      (observe-summary this ::consume-record-summary context (elapsed-time storage context))))

  tracer/IBeforeConsumePartition
  (before-consume-partition
    [this group-id topic-partition records-count]
    (let [context (by-topic-partition group-id topic-partition)]
      (-> (get-storage this ::consume-partition)
          (swap! assoc context (System/nanoTime)))))

  tracer/IAfterConsumePartition
  (after-consume-partition
    [this group-id topic-partition records-count]
    (let [context (by-topic-partition group-id topic-partition)
          storage (get-storage this ::consume-partition)]
      (observe-summary this ::consume-partition-summary context (elapsed-time storage context))))

  tracer/IOnConsumePartitionError
  (on-consume-partition-error
    [this group-id topic-partition records-count exception]
    (let [context (by-topic-partition group-id topic-partition)
          storage (get-storage this ::consume-partition)]
      (observe-summary this ::consume-partition-summary context (elapsed-time storage context))))

  tracer/IBeforeCommit
  (before-commit
    [this group-id offsets]
    (let [context (by-group-id group-id)
          storage (get-storage this ::commit-request)]
      (swap! storage assoc context (System/nanoTime))))

  tracer/IAfterCommit
  (after-commit
    [this group-id offsets]
    (let [context (by-group-id group-id)
          storage (get-storage this ::commit-request)]
      (observe-summary this ::commit-request-summary context (elapsed-time storage context))))

  tracer/IAfterPartitionsPaused
  (after-partitions-paused
    [this group-id topic-partitions]
    (doseq [tp topic-partitions]
      (inc-gauge this ::paused-partitions-gauge (by-topic-partition group-id tp))))

  tracer/IAfterPartitionsResumed
  (after-partitions-resumed
    [this group-id topic-partitions]
    (doseq [tp topic-partitions]
      (dec-gauge this ::paused-partitions-gauge (by-topic-partition group-id tp))))

  tracer/IAfterEndOffsets
  (after-end-offsets
    [this group-id offsets]
    (doseq [[tp offset] offsets]
      (set-gauge this ::end-offset-gauge (by-topic-partition group-id tp) offset)))

  tracer/IAfterBeginningOffsets
  (after-beginning-offsets
    [this group-id offsets]
    (doseq [[tp offset] offsets]
      (set-gauge this ::beginning-offset-gauge (by-topic-partition group-id tp) offset)))

  tracer/ICurrentOffsets
  (current-offsets
    [this group-id offsets]
    (doseq [[tp offset] offsets]
      (set-gauge this ::current-offset-gauge (by-topic-partition group-id tp) offset)))

  component/Lifecycle
  (start
    [this]
    (let [by-group-id (into-array ["group_id"])
          by-topic-partition (into-array ["group_id" "topic" "partition"])]
      (assoc this
        ::storage
        {::poll-request (atom {})
         ::consume-record (atom {})
         ::consume-partition (atom {})
         ::consume (atom {})
         ::commit-request (atom {})}

        ::prometheus
        {::poll-request-summary
         (register-summary
           "kafka_poll_request"
           by-group-id
           "Poll request duration")

         ::poll-interval-summary
         (register-summary
           "kafka_poll_interval"
           by-group-id
           "Interval between subsequent polls")

         ::poll-records-count-summary
         (register-summary
           "kafka_poll_records_count"
           by-group-id
           "Records count returned from poll")

         ::consume-record-summary
         (register-summary
           "kafka_consume_record"
           by-topic-partition
           "Consume record duration")

         ::consume-partition-summary
         (register-summary
           "kafka_consume_partition"
           by-topic-partition
           "Consume partition duration")

         ::consume-summary
         (register-summary
           "kafka_consume"
           by-group-id
           "Consume duration")

         ::commit-request-summary
         (register-summary
           "kafka_commit_request"
           by-group-id
           "Commit request duration")

         ::failed-consumers-counter
         (register-counter
           "kafka_failed_consumers"
           by-group-id
           "Currently failed consumers counter")

         ::current-offset-gauge
         (register-gauge
           "kafka_current_offset"
           by-topic-partition
           "Current offset")

         ::end-offset-gauge
         (register-gauge
           "kafka_end_offset"
           by-topic-partition
           "End offset")

         ::beginning-offset-gauge
         (register-gauge
           "kafka_beginning_offset"
           by-topic-partition
           "Beginning offset")

         ::paused-partitions-gauge
         (register-gauge
           "kafka_paused_partitions"
           by-topic-partition
           "Paused partitions")})))
  (stop
    [this]
    (.clear CollectorRegistry/defaultRegistry)
    (dissoc this ::storage ::prometheus)))
