# Why "keep users/items with ≥5 ratings" needs iteration

This explains a subtle data-engineering point in `src/01_data_engineering.py`:
why a **single-pass** ≥5 filter does *not* actually guarantee that every user and
item has ≥5 ratings, and how an **iterative** filter fixes it.

---

## The goal

For collaborative filtering (ALS), we want to drop users and items that have too
few ratings — they cause cold-start problems and add noise. The rule we want:

> Every user in the final data has ≥5 ratings, **and** every item has ≥5 ratings.

## What the single-pass code actually does

```python
u_keep = users with >= 5 ratings   # counted on the ORIGINAL data
i_keep = items with >= 5 ratings   # counted on the ORIGINAL data
ratings = ratings.join(u_keep).join(i_keep)   # keep rows where BOTH hold
```

Both `u_keep` and `i_keep` are computed **independently, from the same original
table, at the same moment.** Then both are applied as a join.

The flaw: **applying the item filter changes each user's count, but we never recount.**

## A concrete example

User **U** rated exactly 5 products: A, B, C, D, E.

1. At count time U has 5 ratings → **U passes** the user filter. ✓
2. But products **C, D, E** were each rated by only 1–2 people total → they **fail**
   the item filter and are removed.
3. After the join, U's ratings on C, D, E are deleted. U is left with **A, B = 2
   ratings** — below the threshold — but we never re-checked U.

That is exactly why the validation step reports `min ratings/user = 1` even though
we "filtered to ≥5".

## Why it cascades (the filters fight each other)

The two filters interact in both directions:

```
remove low-count items  ->  some users drop below 5
remove those users      ->  some items drop below 5
remove those items      ->  more users drop below 5
...
```

A single pass only resolves the **first** round of this chain reaction.

## The fix: iterate to a fixed point

Keep alternating the two filters until a full pass removes nothing — i.e. the data
stops changing. At that stable point every surviving user and item has ≥5.

```python
prev = -1
while ratings.count() != prev:        # loop until the row count stabilizes
    prev = ratings.count()
    u_keep = ratings.groupBy("user_id").count().filter("count >= 5").select("user_id")
    ratings = ratings.join(u_keep, "user_id")
    i_keep = ratings.groupBy("parent_asin").count().filter("count >= 5").select("parent_asin")
    ratings = ratings.join(i_keep, "parent_asin")
```

Each iteration shrinks the data a little more, then it settles (usually after a few
passes). Once stable, the `>= 5` rule is genuinely true — so it can be promoted
from an informational line to a **hard assertion** in `validate()`.

### Graph-theory name
This stable subset is the **k-core** (here, 5-core) of the user–item *bipartite
graph*: the largest subgraph in which every node has degree ≥ k. Iterative
threshold filtering is the standard way to compute it.

## Trade-off (why the project uses single-pass by default)

| | Single-pass (current) | Iterative (k-core) |
|---|---|---|
| Guarantees ≥5 for all? | No (min can be 1) | Yes |
| Passes over data | 1 | several (slower) |
| Final size | larger | smaller (denser) |
| Assertion possible? | report as info only | can assert ≥5 |

For a course project the single pass is acceptable — it still removes the bulk of
the long tail, and being **honest** about its limit (reporting `min`, not asserting
a false guarantee) is itself good engineering. The iterative version is the more
correct choice if you want a clean, dense matrix and a true ≥5 invariant.

> Takeaway: a filter that *selects* rows can be invalidated by a later filter that
> *removes* the rows it depended on. When two filters depend on each other's output,
> you must iterate to a fixed point before you can assert the property holds.
