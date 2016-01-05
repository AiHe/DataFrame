# DataFrame

## High Performance Column Based DataFrame

This is an implementation of immutable **DataFrame** which supports functional computations.
 
## Primary Features include:
  1. **Immutability**
  2. **Schema Inference**
  3. **Functional Support**

More features are still considered.

This implementation does not take advantage of the type system of Scala(JVM) when inferring the schema because of the 
performance issue. The native Scala type system based on reflection reduces the performance quite a lot.

A abstraction layer on the top of native types is introduced like what __Apache Spark__ does. This hierarchical 
abstraction layer not only enhances the performance significantly, but also make the code more elegant.

