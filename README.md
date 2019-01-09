Demonstrates use of a T-SQL recursive CTE to traverse a hierarchical audit trail.

The SQL file creates a scenario which is based on a real world problem in updating a data warehousing solution from an audit trail.

The data models a waiting list. A waiting list has many entries, however entries can transfer between lists which is captured in the audit trail.  The audit trail stores a reference to the list the entry moved from and the list the entry moved to.

A waiting list entry may have moved lists multiple times. If a waiting list entry is found to be out of date it is necessary to traverse the audit trail in a recursive fashion in order to find the latest values.