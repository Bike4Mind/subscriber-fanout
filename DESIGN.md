# subscriber-fanout - Design Notes

Goal: The client subscribes to MongoDB queries in order to replicate the backend database into the client.

General plan:

- Client adds a `useCollectionQuery(collection, mongoQuery)` hook, which kicks off a query and returns matching records
- The query initially gathers data from the in-browser database, and simultaneously opens a query to the backend for data.
- The backend processes these queries and arranges for ongoing change stream results to be delivered to the client.

## Client Handling

The client exposes a hook, `useCollectionQuery(collection, mongoQuery)`, which returns the records pertinent to the query.  It arranges for a query with the websocket backend, then returns the reactive result-set for a query against IndexedDB.

Once the query has subscribed to the backend, it begins receiving records, starting with any existing records in the database which match the query.  As the client receives these records, it'll insert them into the local database.  Once inserted, they'll be returned by the query which requested them.

## Server handling

We need to have _some_ long-running process going, to consume the change-stream.  Therefore, we implement a long-running backend, subscriber-fanout, which issues queries to MongoDB, listens to the resulting change streams, and sends changes to interested clients.

Thinking through the subscription lifecycle:

- Client makes a websocket request which goes to a Lambda Function, `subscribe_query`, to arrange the subscription
- This function queries for the query requested, then associates a normalized form of the query with destination socket ID.  There may be multiple subscribers to the same query.  These associations are stored in MongoDB itself.
- Our long-running process monitors mongo's querySubscriptions collection for new queries to monitor, and as they're added/created, our process begins monitoring them.  Initially, it monitors all queries, but in the future we may provide a load-balancing mechanism between multiple instances of our long-running process.
- Having subscribed to the query of interest, the long-running process receives updates and dispatches them to subscribers.
- As it does so, if any of the subscribers have disconnected and throws an error, we remove it from the subscription list.  Once the subscription list becomes empty, we drop the subscription.

## Q&A

- If we used DocumentDB, does it support change streams, and can it dispatch to functions?
  - Yes, via Amazon EventSources, but then obviously we're pretty locked into DocumentDB.
  - Internally, we could have our own processing function, dispatched either by a long-running (change-stream) subscription, or by an Amazon EventSource linked to DocumentDB.

