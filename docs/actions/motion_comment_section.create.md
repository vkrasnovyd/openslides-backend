## Payload
```js
{
// Required
    name: string;
    meeting_id: Id;

// Optional
    read_group_ids: Id[];
    write_group_ids: Id[];
    submitter_can_write: boolean;
}
```

## Action
Creates a new comment section. The `weight` must be set to `max+1` of all comment sections of the meeting. The given groups must belong to the same meeting.

The `write_group_ids` may not contain the meetings `anonymous_group_id`.

## Permissions
The request user needs `motion.can_manage`.
