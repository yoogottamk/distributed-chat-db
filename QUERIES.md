# Queries related to the application

## Get all group names the user is a part of
Given user id $ID,

```sql
select G.`name` from `group` G, `group_member` GM where GM.`user` = $ID and G.`id` = GM.`group`;
```

## Get all groups the user is an admin of
Given user id $ID,

```sql
select * from `group` where `created_by` = $ID;
```

## Get author name, time and content of messages of a group
Given group id $ID,

```sql
select U.`name`, M.`sent_at`, M.`content`
  from `message` M, `user` U
where
  M.`group` = $ID and
  M.`author` = U.id;
```

## Get all unseen messages of a user
Given user id $ID,

```sql
select G.`name`, M.`content`
  from `group` G, `message` M, `group_member` GM, `user` U
where
  GM.`user` = $ID and
  U.`id` = $ID and
  GM.`group` = G.`id` and
  M.`group` = G.`id` and
  M.`sent_at` > U.`last_seen`;
```
