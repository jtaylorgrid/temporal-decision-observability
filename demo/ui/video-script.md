# Decision Observability Demo - 90s Video Script

**Target audience:** Grid Dynamics colleagues working on Temporal.io client projects

---

## Script (~90 seconds)

**[Opening - 0:00-0:15]**

If you're building AI agents with Temporal, you've probably heard this question from a client:

*"When something goes wrong, can we prove what the agent knew when it made that decision?"*

And honestly? Temporal alone can't answer that. Not properly.

**[Problem - 0:15-0:35]**

Here's why: Temporal is brilliant at orchestration. It tracks *what* happened. But it doesn't track *what was known* at the moment a decision was made.

Think about it - your agent pulls a churn score from a model, routes a customer to a queue... but that model gets updated. The original score? Gone. Overwritten.

When compliance asks "why did you route this VIP customer to the slow queue?" - you're stuck.

**[Solution - 0:35-0:55]**

This demo shows a different approach. We stream Temporal events into XTDB - a bitemporal database.

Bitemporal means two time dimensions: *when things happened* and *when we recorded them*. So you can query: "show me exactly what the system believed at 2pm last Tuesday."

Model corrections arrive late? No problem. You can see the before *and* after - permanently.

**[Demo highlights - 0:55-1:15]**

What you're looking at:
- Real Temporal workflow events flowing through Kafka Connect into XTDB
- Audit queries that join workflow decisions with ML predictions *at the exact moment they occurred*
- Counterfactual analysis - what *should* have happened if data propagated faster
- Full payload capture - every activity input and output, preserved forever

**[Close - 1:15-1:30]**

If you're on a project using Temporal for anything decision-sensitive - customer routing, fraud detection, agent orchestration - this solves a real compliance gap.

Let's chat. I can walk you through the architecture and we can explore if this fits your client's needs.

---

## Key talking points if questions come up

- **"Isn't Temporal's event history enough?"** - Only for 30-90 days, and it doesn't capture external data (ML predictions, CRM state) at decision time
- **"Why XTDB?"** - SQL interface, bitemporal queries are first-class, PostgreSQL wire protocol means existing tools just work
- **"How hard is this to add?"** - It's a CDC connector. Doesn't change your Temporal code at all.
- **"What about scale?"** - XTDB handles millions of events. The connector is stateless and horizontally scalable.
