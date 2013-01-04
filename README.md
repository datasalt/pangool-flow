Pangool-flow
============

Pangool-flow is an experimental module on top of Pangool (http://pangool.net).

Pangool-flow adds to Pangool:
- The possibility of chaining Pangool MapReduce Jobs and executing the resultant flow.
- Parallel execution of Jobs in a Flow.
- High-level constructs (operations) similar to those in Cascading, called "Ops".

The difference between Pangool-flow and other flow-based APIs such as Pig or Cascading is that Pangool-flow is built around the MapReduce abstraction.
So, each step in the flow is represented by a MapReduceStep. Therefore, it is possible to tune every MapReduceStep as much as needed, and so there is no flexibility tradeoff involved in using pangool-flow.
