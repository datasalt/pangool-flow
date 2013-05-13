; This is work-in-progress playground for integration Clojure with pangool-flow
; TODO for some reason multi-level namespaces don't work (understand why)
(ns test
  (:require clojure.data.json)
  (:gen-class))

(import 'com.datasalt.pangool.flow.MRStep)
(import 'com.datasalt.pangool.flow.GroupBy)
(import 'com.datasalt.pangool.flow.ResourceMRInput)
(import 'com.datasalt.pangool.flow.MapReduceFlowBuilder)
(import 'com.datasalt.pangool.flow.BaseFlow)
; inner classes -> $ !
(import 'com.datasalt.pangool.flow.BaseFlow$EXECUTION_MODE)
(import 'com.datasalt.pangool.flow.io.TupleInput)
(import 'com.datasalt.pangool.flow.io.TupleOutput)
(import 'com.datasalt.pangool.flow.io.TextInput)
(import 'com.datasalt.pangool.flow.io.TextOutput)
(import 'com.datasalt.pangool.flow.mapred.TextMapper)
(import 'com.datasalt.pangool.flow.mapred.TupleOpMapper)
; why can't we use .* ?
(import 'com.datasalt.pangool.flow.mapred.TupleOpReducer)
(import 'com.datasalt.pangool.flow.ops.ClojureOp)
(import 'com.datasalt.pangool.flow.ops.TopTuples)

(import 'org.apache.hadoop.conf.Configuration)

; http://dev.clojure.org/display/design/Where+Did+Clojure.Contrib+Go
; https://github.com/clojure/data.json/

; framework methods

(defn identity-op [input, callback] input)

(defn identity-reduce-op [input, callback]
  (let [seq-input (seq input)] ; This should be done by the Java wrapper
    (doall (map (fn [tuple] (.onReturn callback tuple)) seq-input))
    nil))

(defn input-for [input-spec]
  (let [input-format (or (:format input-spec) :tuple) ;default is tuple
        cl-op (new ClojureOp 
                   (or (:op input-spec) identity-op) ; or the user function, or the identity one 
                   (or (:output-schema input-spec) (.getSchema (:input input-spec))))] ; or a explicit schema, or the one associated with the input
  (cond 
    (= input-format :text) (new TextInput (new TextMapper cl-op))
    (= input-format :tuple) (new TupleInput (new TupleOpMapper cl-op))
  )))

(defn create-step [def]
  (let [step-name (or (:name def) "unnamed-step")
        step (new MRStep step-name (class *ns*))
        reducer-spec (:reducer def)
        out-schema (:output-schema reducer-spec)
        reducer-op (or (:op reducer-spec) identity-reduce-op)]
    (doall (map (fn [input-spec] (.addInput step (:input input-spec) (input-for input-spec))) (:inputs def)))
    (doto step 
      (.groupBy (new GroupBy (into-array (clojure.string/split (:group-by def) #","))))
      (.setReducer (new TupleOpReducer 
                        (cond out-schema (new ClojureOp reducer-op out-schema)
                          :else (new ClojureOp reducer-op)))))))

; framework methods

; tweet example ---------

(def tweet-schema "date:string?,text:string,screen_name:string,location:string?")
(def count-schema "screen_name:string,count:int")
(def flow-input (new ResourceMRInput "src/test/resources/barackobama_tweets.json"))
(def hadoop-conf (new Configuration))
(def execution-mode (BaseFlow$EXECUTION_MODE/OVERWRITE))

(defn tweet-parse-op [input, callback]
  (let [parsed-tweet (clojure.data.json/read-json input)]
    (list 
      (:created_at parsed-tweet) 
      (:text parsed-tweet) 
      (:screen_name (:user parsed-tweet))
      (:location (:user parsed-tweet)))))

(defn reduce-count-op [input, callback]
  (let [seq-input (seq input)] ; This should be done by the Java wrapper
    (list (.get (first seq-input) 0) (count seq-input))))

(def step-1-def 
  {:name "step-1"
   :inputs [{:input flow-input :format :text :op tweet-parse-op :output-schema tweet-schema}]
   :group-by "screen_name"
   :reducer {:op reduce-count-op :output-schema count-schema}
   })

(def step-1 (create-step step-1-def))
(def step-1-out (.setOutput step-1 (new TupleOutput count-schema)))

(def step-2-def
  {:name "step-2"
   :inputs [{:input step-1-out}]
   :group-by "screen_name"
  })

(def step-2 (create-step step-2-def))
(def step-2-out (.setOutput step-2 (new TextOutput)))

(defn create-flow []
  (let [flow-builder (new MapReduceFlowBuilder)]
    (doto flow-builder 
      (.addInput flow-input)
      (.addStep step-1)
      (.addStep step-2)
      (.bindOutput step-2-out "out-clj"))))

(defn run-flow []
  (doto (create-flow) (.execute execution-mode hadoop-conf (into-array ["out-clj"]))))