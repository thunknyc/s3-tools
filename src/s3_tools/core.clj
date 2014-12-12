(ns s3-tools.core
  (:require [aws.sdk.s3 :as s3]))

(defn- adjust-options
  [options new-marker n-read]
  (-> (if (contains? options :max-keys)
        (update-in options [:max-keys] - n-read)
        options)
    (assoc :marker new-marker)))

(defn- object-seq*
  [cred bucket options xs]
  (if (seq xs)
    (cons (first xs)
          (lazy-seq (object-seq* cred bucket options (rest xs))))
    (let [{:keys [next-marker objects]}
          (s3/list-objects cred bucket options)
          n-objects (count objects)]
      (if (seq objects)
        (recur cred
               bucket
               (adjust-options options next-marker n-objects)
               objects)
        nil))))

(defn object-seq
  "Return a lazy sequence of objects accepting the same options as
  `aws.sdk.s3/list-objects`, whose docs you should read and understand
  in order to properly use. The sequence returned will page through
  multiple results if necessary."
  [cred bucket & [options]]
  (object-seq* cred bucket options nil))

