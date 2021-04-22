# database-connection-examples

A repo demonstrating how to efficiently connect to datastores inside your Spark application.

## Methods for connecting to a database inside spark

### Initializing the connection on the driver ( aka the wrong way )

Often a beginnings first step when writing Spark code. Initializing the connection on teh driver, and trying to share it
with each record on the executor, won't work. The connection isn't serializable, and will throw an exception.

### Opening the connection on the executor for each row

The next logical progression is to move the opening of the database connection onto the executor by placing it inside
.forEach or a .map. This works! However, is wasteful because you open a connection for each row. Resulting in a lot of
unnecessary overhead.

### Opening a connection per partition

By using .mapPartition or .forEachPartition, we can open a single connection per partition ( per batch ), and re-use
that connection for each of that batch's row on that partition.

### [To add] Re-using connections across batches by implementing a connection pool.

### [To add] Multiplex rows into fewer get requests.

Re-use connections across batches.
