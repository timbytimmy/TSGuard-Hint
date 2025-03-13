#### Time series algebra expression mapping rules.

|  ID  | SQL expression                    | Time Series Algebra                | Explanation                              |
| :--: | --------------------------------- | ---------------------------------- | ---------------------------------------- |
|  1   | $$ T < c $$                       | $T_{vc} =\{ (t,v)|t∈T,v∈(-∞,c) \}$ | Select elements in T where values are < c. |
|  2   | $$ T > c $$                       | $T_{vc}=\{(t,v)|t∈T,v∈(c,∞)\} $    | Select elements in T where values are > c. |
|  3   | $$ T = c $$                       | $T_{vc}=\{(t,v)|t∈T,v∈[c,c]\}$     | Select elements in T where values are = c. |
|  4   | $$ T \leq c $$                    | $T_{vc}=\{(t,v)|t∈T,v∈(-∞,c]\}$    | Select elements in T where values are ≤ c. |
|  5   | $$ T \geq c $$                    | $T_{vc}=\{(t,v)|t∈T,v∈[c,∞)\}$     | Select elements in T where values are ≥ c. |
|  6   | $$ T \neq c $$                    | $T_{vc}=\{(t,v)|t∈T,v≠c\}$         | Select elements in T where values are ≠ c. |
|  7   | $$true$$                          | $T_{vc}=\{(t,v)|t∈T\}$             | Select all elements.                     |
|  8   | $$false$$                         | $T_{vc}=∅$                         | Select no elements.                      |
|  9   | $T\;IS\;NOT\;NULL$                | $T_{vc}=\{(t,v)|t∈T,v≠null\}$      | Select elements in T where values are not null. |
|  10  | $T\;IS\;NULL$                     | $T_{vc}=\{(t,v)|t∈T,v=null\}$      | Select elements in T where values are null. |
|  11  | $ T \; BETWEEN \; a \; AND \; c $ | $T_{vc}=\{(t,v)|t∈T,v∈[a,c]\}$     | Select elements in T where values are between a and c. |
|  12  | $T \; IN \; (a,\;b,\;c,\;...)$    | $T_{vc}=\{(t,v)|t∈T,v∈(a,b,c…)\}$  | Select elements in T where values are in (a,b,c...). |
|  13  | $$+ \; T$$                        | $T=+T$                             | Select elements in T where values are = + v. |
|  14  | $$- \; T$$                        | $T=-T$                             | Select elements in T where values are = - v. |
|  15  | $$T_{b} + T_{c}$$                 | $T_a=T_b+T_c$                      | Adds the values of $T_{bc}$ and $T_{bc}$. |
|  16  | $$T_{b} - T_{c}$$                 | $T_a=T_b-T_c$                      | Subtracts the value of $T_{bc}$ from $T_{cc}$. |
|  17  | $$T_{b} \; * \; T_{c}$$           | $T_a=T_b*T_c$                      | Multiplies the values of $T_{bc}$ and $T_{cc}$. |
|  18  | $$T_{b} \; / \; T_{c}$$           | $T_a=T_b/T_c$                      | Divides the value of $T_{bc}$by $T_{cc}$. |
|  19  | $T_{b}\mod\;T_{c}$                | $T_a=T_b\mod T_c$                  | Returns the remainder after dividing $T_{bc}$ by $T_{cc}$. |
|  20  | $T_{bc} \; AND \; T_{cc}$         | $T_{ac}=T_{bc}∩T_{cc}$             | Compute the intersection of $T_{bc}$ and $T_{cc}$, where $T_{bc}$ and $T_{cc}$ are constraints and $T_{ac}$ is the result. |
|  21  | $T_{bc} \; OR \; T_{cc}$          | $T_{ac}=T_{bc}∪T_{cc}$             | Compute the union of $T_{bc}$ and $T_{cc}$. |
|  22  | $NOT \; T_{bc}$                   | $T_{ac}=¬T_{bc}$                   | Compute the complement of constraint $T_{bc}$. |
|  23  | $ABS(T)$                          | $T=ABS(T)$                         | Returns the absolute value of $T$, which is its non-negative magnitude. |
|  24  | $ACOS(T)$                         | $T=ACOS(T)$                        | Computes the inverse cosine of $T$, returning an angle (in radians) whose cosine is $T$. |
|  25  | $ASIN(T)$                         | $T=ASIN(T)$                        | Computes the inverse sine of $T$, returning an angle (in radians) whose sine is $T$. |
|  26  | $ATAN(T)$                         | $T=ATAN(T)$                        | Computes the inverse tangent of $T$, returning an angle (in radians) whose |
|  27  | $SQRT(T)$                         | $T=SQRT(T)$                        | Computes the non-negative square root of $T$. |
|  28  | $W\_SUM(T)$                       | $T_{ac}=W\_SUM(T)$                 | Calculate the cumulative sum of columns $T$ within the time window. |
|  29  | $W\_COUNT(T)$                     | $T_{ac}=W\_COUNT(T)$               | Counts the number of rows in the time window. |
|  30  | $W\_AVG(T)$                       | $T_{ac}=W\_AVG(T)$                 | Calculate the arithmetic mean of column $T$ within the time window. |
|  31  | $W\_SPEAD(T)$                     | $T_{ac}=W\_SPEAD(T)$               | Calculate the extreme difference (the difference between the maximum and minimum values) of column $T$ within the time window, i.e. W_MAX(T) - W_MIN(T). |
|  32  | $W\_STDDEV\_POP(T)$               | $T_{ac}=W\_STDDEV\_POP(T)$         | Calculate the overall standard deviation (based on the standard deviation of the whole data) of column $T$ within the time window. |
|  33  | $W\_VAR\_POP(T)$                  | $T_{ac}=W\_VAR\_POP(T)$            | Calculate the overall variance, i.e., the square of the overall standard deviation, for column $T$ within the time window. |
|  34  | $W\_MAX(T)$                       | $T_{ac}=W\_MAX(T)$                 | Returns the maximum value of column $T$ within the time window. |
|  35  | $W\_MIN(T)$                       | $T_{ac}=W\_MIN(T)$                 | Returns the minimum value of column $T$ within the time window. |
|  36  | $W\_FIRST(T)$                     | $T_{ac}=W\_FIRST(T)$               | Returns the first non-null value of column $T$ in the time window (in chronological order). |
|  37  | $W\_LAST(T)$                      | $T_{ac}=W\_LAST(T)$                | Returns the last non-null value of column $T$ in the time window (in chronological order). |
|  38  | $T\_F\_CSUM(T)$                   | $T_{ac}=T\_F\_CSUM(T)$             | Computes the cumulative sum of column `T` over a time-ordered sequence. |
|  39  | $T\_F\_DIFF(T)$                   | $T_{ac}=T\_F\_DIFF(T)$             | Computes the discrete difference between the current value of `T` and its previous value in the time sequence. |
|  40  | $T\_F\_DERIVATIVE(T)$             | $T_{ac}=T\_F\__DERIVATIVE(T)$      | Computes the rate of change (derivative) of `T` over time, i.e., the difference in `T` divided by the time interval. |
|  41  | $T\_F\_MAVG(T)$                   | $T_{ac}=T\_F\_MAVG(T)$             | Computes the moving average (rolling average) of column `T` over the last `N` time-ordered values. |
|  42  | $T\_F\_STATECOUNT(T)$             | $T_{ac}=T\_F\_STATECOUNT(T)$       | Counts the number of consecutive occurrences where column `T` meets a specified `condition` (e.g., `T > threshold`) in the time sequence. |
|  43  | $T\_F\_STATEDURATION(T)$          | $T_{ac}=T\_F\_STATEDURATION(T)$    | Computes the total duration (in specified `time_unit`, e.g., seconds) during which column `T` satisfies a `condition` in the time sequence. |

