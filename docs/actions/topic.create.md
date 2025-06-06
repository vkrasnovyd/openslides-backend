## Payload
```js
{
// Required
    title: string;
    meeting_id: Id;

// Optional
    text: HTML;
    attachment_mediafile_ids: Id[];

    // Non-model fields for customizing the agenda item creation
    agenda_create: boolean;
    agenda_type: number;
    agenda_parent_id: number;
    agenda_comment: string;
    agenda_duration: number;
    agenda_weight: number;
    agenda_tag_ids: Id[];
}
```

## Action
Note: `attachment_mediafile_ids` must be from the same meeting or the mediafiles must be published. For the agenda fields see
[Agenda](https://github.com/OpenSlides/OpenSlides/wiki/Agenda#additional-fields-during-creation-of-agenda-content-objects).

## Permissions
The request user needs `agenda_item.can_manage`.
