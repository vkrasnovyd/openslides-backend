import time
from collections import defaultdict
from typing import Any

from openslides_backend.action.actions.motion.mixins import TextHashMixin
from openslides_backend.shared.typing import HistoryInformation

from ....permissions.permission_helper import has_perm
from ....permissions.permissions import Permissions
from ....services.datastore.commands import GetManyRequest
from ....shared.exceptions import ActionException, PermissionDenied
from ....shared.filters import FilterOperator
from ....shared.interfaces.write_request import WriteRequest
from ....shared.patterns import fqid_from_collection_and_id
from ...util.typing import ActionData, ActionResultElement, ActionResults
from ..motion_change_recommendation.create import MotionChangeRecommendationCreateAction
from .create_base import MotionCreateBase
from openslides_backend.shared.interfaces.event import Event, EventType
from collections.abc import Iterable


class BaseMotionCreateForwarded(TextHashMixin, MotionCreateBase):
    """
    Base create action for forwarded motions.
    """

    def prefetch(self, action_data: ActionData) -> None:
        self.datastore.get_many(
            [
                GetManyRequest(
                    "meeting",
                    list(
                        {
                            meeting_id
                            for instance in action_data
                            if (meeting_id := instance.get("meeting_id"))
                        }
                    ),
                    [
                        "id",
                        "is_active_in_organization_id",
                        "name",
                        "motions_default_workflow_id",
                        "motions_default_amendment_workflow_id",
                        "committee_id",
                        "default_group_id",
                        "motion_submitter_ids",
                        "motions_number_type",
                        "motions_number_min_digits",
                        "agenda_item_creation",
                        "list_of_speakers_initially_closed",
                        "list_of_speakers_ids",
                        "motion_ids",
                    ],
                ),
                GetManyRequest(
                    "motion",
                    list(
                        {
                            origin_id
                            for instance in action_data
                            if (origin_id := instance.get("origin_id"))
                        }
                    ),
                    [
                        "meeting_id",
                        "lead_motion_id",
                        "statute_paragraph_id",
                        "state_id",
                        "all_origin_ids",
                        "derived_motion_ids",
                        "all_derived_motion_ids",
                        "amendment_ids",
                        "attachment_meeting_mediafile_ids",
                    ],
                ),
            ],
            lock_result=False,
        )

    def get_user_verbose_names(self, meeting_user_ids: list[int]) -> str | None:
        meeting_users = self.datastore.get_many(
            [
                GetManyRequest(
                    "meeting_user", meeting_user_ids, ["user_id", "structure_level_ids"]
                )
            ],
            lock_result=False,
        )["meeting_user"]
        user_ids = [
            user_id
            for meeting_user in meeting_users.values()
            if (user_id := meeting_user.get("user_id"))
        ]
        if not len(user_ids):
            return None
        requests = [
            GetManyRequest(
                "user", user_ids, ["id", "first_name", "last_name", "title", "pronoun"]
            )
        ]
        if structure_level_ids := list(
            {
                structure_level_id
                for meeting_user in meeting_users.values()
                for structure_level_id in meeting_user.get("structure_level_ids", [])
            }
        ):
            requests.append(
                GetManyRequest("structure_level", structure_level_ids, ["name"])
            )
        user_data = self.datastore.get_many(requests, lock_result=False)
        users = user_data["user"]
        structure_levels = user_data["structure_level"]
        names = []
        for meeting_user_id in meeting_user_ids:
            meeting_user = meeting_users[meeting_user_id]
            user = users.get(meeting_user.get("user_id", 0))
            if user:
                additional_info: list[str] = []
                if pronoun := user.get("pronoun"):
                    additional_info = [pronoun]
                if sl_ids := meeting_user.get("structure_level_ids"):
                    if slnames := ", ".join(
                        name
                        for structure_level_id in sl_ids
                        if (
                            name := structure_levels.get(structure_level_id, {}).get(
                                "name"
                            )
                        )
                    ):
                        additional_info.append(slnames)
                suffix = " · ".join(additional_info)
                if suffix:
                    suffix = f"({suffix})"
                if not any(user.get(field) for field in ["first_name", "last_name"]):
                    short_name = f"User {user['id']}"
                else:
                    short_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                long_name = f"{user.get('title', '')} {short_name} {suffix}".strip()
                names.append(long_name)
        return ", ".join(names)

    def perform(
        self, action_data: ActionData, user_id: int, internal: bool = False
    ) -> tuple[WriteRequest | None, ActionResults | None]:
        self.id_to_result_extra_data: dict[int, dict[str, Any]] = {}
        return super().perform(action_data, user_id, internal)

    def update_instance(self, instance: dict[str, Any]) -> dict[str, Any]:
        meeting = self.datastore.get(
            fqid_from_collection_and_id("meeting", instance["meeting_id"]),
            ["motions_default_workflow_id", "motions_default_amendment_workflow_id"],
            lock_result=False,
        )
        self.set_state_from_workflow(instance, meeting)
        committee = self.check_for_origin_id(instance)
        use_original_number = instance.get("use_original_number", False)

        if use_original_submitter := instance.pop("use_original_submitter", False):
            submitters = list(
                self.datastore.filter(
                    "motion_submitter",
                    FilterOperator("motion_id", "=", instance["origin_id"]),
                    ["meeting_user_id"],
                    lock_result=False,
                ).values()
            )
            submitters = sorted(submitters, key=lambda x: x.get("weight", 10000))
            meeting_user_ids = [
                meeting_user_id
                for submitter in submitters
                if (meeting_user_id := submitter.get("meeting_user_id"))
            ]
            if len(meeting_user_ids):
                instance["additional_submitter"] = self.get_user_verbose_names(
                    meeting_user_ids
                )
            text_submitter = self.datastore.get(
                fqid_from_collection_and_id("motion", instance["origin_id"]),
                ["additional_submitter"],
                lock_result=False,
            ).get("additional_submitter")
            if text_submitter:
                if instance.get("additional_submitter"):
                    instance["additional_submitter"] += ", " + text_submitter
                else:
                    instance["additional_submitter"] = text_submitter
        else:
            name = committee.get("name", f"Committee {committee['id']}")
            instance["additional_submitter"] = name

        self.set_sequential_number(instance)
        self.handle_number(instance)
        self.set_origin_ids(instance)
        self.set_text_hash(instance)
        instance["forwarded"] = round(time.time())
        with_change_recommendations = instance.pop("with_change_recommendations", False)
        self.datastore.apply_changed_model(
            fqid_from_collection_and_id("motion", instance["id"]), instance
        )
        if with_change_recommendations:
            change_recos = self.datastore.filter(
                "motion_change_recommendation",
                FilterOperator("motion_id", "=", instance["origin_id"]),
                [
                    "rejected",
                    "internal",
                    "type",
                    "other_description",
                    "line_from",
                    "line_to",
                    "text",
                ],
            )
            change_reco_data = [
                {**change_reco, "motion_id": instance["id"]}
                for change_reco in change_recos.values()
            ]
            self.execute_other_action(
                MotionChangeRecommendationCreateAction, change_reco_data
            )
        amendment_ids = self.datastore.get(
            fqid_from_collection_and_id("motion", instance["origin_id"]),
            ["amendment_ids"],
            lock_result=False,
        ).get("amendment_ids", [])
        if self.should_forward_amendments(instance):
            new_amendments = self.datastore.get_many(
                [
                    GetManyRequest(
                        "motion",
                        amendment_ids,
                        [
                            "title",
                            "text",
                            "amendment_paragraphs",
                            "reason",
                            "id",
                            "state_id",
                        ],
                    )
                ]
            )["motion"]
            total = len(new_amendments)
            state_ids = {
                state_id
                for amendment in new_amendments.values()
                if (state_id := amendment.get("state_id"))
            }
            if len(state_ids):
                states = self.datastore.get_many(
                    [
                        GetManyRequest(
                            "motion_state",
                            list(state_ids),
                            ["allow_amendment_forwarding"],
                        )
                    ],
                    lock_result=False,
                )["motion_state"]
            else:
                states = {}
            states = {
                id_: state
                for id_, state in states.items()
                if state.get("allow_amendment_forwarding")
            }
            for amendment in list(new_amendments.values()):
                if not (
                    (state_id := amendment.pop("state_id", None)) and state_id in states
                ):
                    new_amendments.pop(amendment["id"])
            amendment_data = new_amendments.values()
            for amendment in amendment_data:
                amendment.update(
                    {
                        "lead_motion_id": instance["id"],
                        "origin_id": amendment["id"],
                        "meeting_id": instance["meeting_id"],
                        "use_original_submitter": use_original_submitter,
                        "use_original_number": use_original_number,
                        "with_change_recommendations": with_change_recommendations,
                    }
                )
                amendment.pop("meta_position", 0)
                amendment.pop("id")
            amendment_results = self.create_amendments(list(amendment_data)) or []
            self.id_to_result_extra_data[instance["id"]] = {
                "non_forwarded_amendment_amount": total - len(amendment_results),
                "amendment_result_data": amendment_results,
            }
        else:
            self.id_to_result_extra_data[instance["id"]] = {
                "non_forwarded_amendment_amount": len(amendment_ids),
                "amendment_result_data": [],
            }

        if self.with_attachments:
            forwarded_mediafiles = self.forward_mediafiles(instance)
            if forwarded_mediafiles.keys():
                instance["attachment_meeting_mediafile_ids"] = [
                    id_ for id_ in forwarded_mediafiles["meeting_mediafile"]
                ]

                for collection, instances in forwarded_mediafiles.items():
                    for id_, instance_ in instances.items():
                        fqid = f"{collection}/{id_}"
                        if instance_.get("meta_new"):
                            self.validate_relation_fields(instance_)
                        self.events.extend(
                            self.create_events(instance_, collection, fqid)
                        )

        return instance

    def create_events(
        self, instance: dict[str, Any], collection: str = "", fqid: str = ""
    ) -> Iterable[Event]:
        """
        Creates events for one instance of the current model.
        """
        collection = collection or self.model.collection
        fqid = fqid or fqid_from_collection_and_id(collection, instance["id"])
        meta_new = instance.pop("meta_new", False)
        event_type = EventType.Create if meta_new else EventType.Update
        yield self.build_event(event_type, fqid, instance)

    def create_amendments(self, amendment_data: ActionData) -> ActionResults | None:
        raise ActionException("Not implemented")

    def create_action_result_element(
        self, instance: dict[str, Any]
    ) -> ActionResultElement | None:
        result = super().create_action_result_element(instance) or {}
        result.update(self.id_to_result_extra_data.get(result["id"], {}))
        return result

    def handle_number(self, instance: dict[str, Any]) -> dict[str, Any]:
        origin = self.datastore.get(
            fqid_from_collection_and_id("motion", instance["origin_id"]),
            ["number"],
            lock_result=False,
        )
        if instance.pop("use_original_number", None) and (num := origin.get("number")):
            number = self.get_clean_number(num, instance["meeting_id"])
            self.set_created_last_modified(instance)
            instance["number"] = number
        else:
            self.set_created_last_modified_and_number(instance)
        return instance

    def get_clean_number(self, number: str, meeting_id: int) -> str:
        new_number = number
        next_identifier = 1
        while not self._check_if_unique(new_number, meeting_id, None):
            new_number = f"{number}-{next_identifier}"
            next_identifier += 1
        return new_number

    def check_for_origin_id(self, instance: dict[str, Any]) -> dict[str, Any]:
        meeting = self.datastore.get(
            fqid_from_collection_and_id("meeting", instance["meeting_id"]),
            ["committee_id"],
            lock_result=False,
        )
        forwarded_from = self.datastore.get(
            fqid_from_collection_and_id("motion", instance["origin_id"]),
            ["meeting_id"],
            lock_result=False,
        )
        forwarded_from_meeting = self.datastore.get(
            fqid_from_collection_and_id("meeting", forwarded_from["meeting_id"]),
            ["committee_id"],
            lock_result=False,
        )
        # use the forwarding user id and id later in the handle forwarding user
        # code.
        committee = self.datastore.get(
            fqid_from_collection_and_id(
                "committee", forwarded_from_meeting["committee_id"]
            ),
            ["id", "name", "forward_to_committee_ids"],
            lock_result=False,
        )
        if meeting["committee_id"] not in committee.get("forward_to_committee_ids", []):
            raise ActionException(
                f"Committee id {meeting['committee_id']} not in {committee.get('forward_to_committee_ids', [])}"
            )
        return committee

    def should_forward_amendments(self, instance: dict[str, Any]) -> bool:
        raise ActionException("Not implemented")

    def check_permissions(self, instance: dict[str, Any]) -> None:
        origin = self.datastore.get(
            fqid_from_collection_and_id(self.model.collection, instance["origin_id"]),
            ["meeting_id"],
            lock_result=False,
        )
        perm_origin = Permissions.Motion.CAN_FORWARD
        if not has_perm(
            self.datastore, self.user_id, perm_origin, origin["meeting_id"]
        ):
            msg = f"You are not allowed to perform action {self.name}."
            msg += f" Missing permission: {perm_origin}"
            raise PermissionDenied(msg)

    def set_origin_ids(self, instance: dict[str, Any]) -> None:
        if instance.get("origin_id"):
            origin = self.datastore.get(
                fqid_from_collection_and_id("motion", instance["origin_id"]),
                ["all_origin_ids", "meeting_id"],
                lock_result=False,
            )
            instance["origin_meeting_id"] = origin["meeting_id"]
            instance["all_origin_ids"] = origin.get("all_origin_ids", [])
            instance["all_origin_ids"].append(instance["origin_id"])

    @staticmethod
    def is_orga_wide(mediafile: dict) -> bool:
        return "organization" in mediafile.get("owner_id", "")

    def forward_mediafiles(self, instance: dict[str, Any]) -> None:
        # Extract data
        origin_attachment_ids = self.datastore.get(
            fqid_from_collection_and_id("motion", instance["origin_id"]),
            ["attachment_meeting_mediafile_ids"],
            lock_result=False,
        ).get("attachment_meeting_mediafile_ids", [])

        origin_meeting_mediafiles = self.datastore.get_many(
            [
                GetManyRequest(
                    "meeting_mediafile",
                    origin_attachment_ids,
                    ["mediafile_id", "meeting_id", "is_public", "attachment_ids"],
                )
            ],
            lock_result=False,
        )["meeting_mediafile"]

        origin_mediafile_ids = [
            mm.get("mediafile_id") for mm in origin_meeting_mediafiles.values()
        ]

        origin_mediafiles = self.datastore.get_many(
            [
                GetManyRequest(
                    "mediafile",
                    origin_mediafile_ids,
                    [
                        "id",
                        "title",
                        "is_directory",
                        "filesize",
                        "filename",
                        "mimetype",
                        "pdf_information",
                        "token",
                        "published_to_meetings_in_organization_id",
                        "parent_id",
                        "child_ids",
                        "owner_id",
                        "meeting_mediafile_ids",
                    ],
                )
            ],
            lock_result=False,
        )["mediafile"]

        meeting_wide_mediafiles = [
            mediafile
            for mediafile in origin_mediafiles.values()
            if not self.is_orga_wide(mediafile)
        ]

        new_mediafiles_ids = (
            iter(self.datastore.reserve_ids("mediafile", len(meeting_wide_mediafiles)))
            if meeting_wide_mediafiles
            else iter([])
        )

        # Create replace map
        replace_map: dict[int, int] = {
            origin_mediafile_id: (
                next(new_mediafiles_ids)
                if not self.is_orga_wide(mediafile)
                else origin_mediafile_id
            )
            for origin_mediafile_id, mediafile in origin_mediafiles.items()
        }

        # Replace ids, update instances
        new_mediafiles: dict[str, Any] = {}

        for origin_id, new_id in replace_map.items():
            meeting_id = instance["meeting_id"]
            origin_mediafile = origin_mediafiles.get(origin_id)
            if self.is_orga_wide(origin_mediafile):
                new_mediafiles[origin_id] = origin_mediafile
            else:
                if not origin_mediafile.get("is_directory"):
                    self.media.duplicate_mediafile(origin_id, new_id)
                new_mediafiles[new_id] = {
                    **origin_mediafile,
                    "id": new_id,
                    "owner_id": f"meeting/{meeting_id}",
                    "meta_new": True,
                }
                parent_id = origin_mediafile.pop("parent_id", None)
                child_ids = origin_mediafile.pop("child_ids", [])
                if parent_id:
                    origin_mediafile["parent_id"] = replace_map.get(parent_id)
                if len(child_ids):
                    origin_mediafile["child_ids"] = [
                        replace_map.get(child_id) for child_id in child_ids
                    ]

        if not len(new_mediafiles):
            return

        new_meeting_mediafile_ids = iter(
            self.datastore.reserve_ids("meeting_mediafile", len(new_mediafiles))
        )

        new_meeting_mediafiles: dict[str, Any] = {}

        for origin_meeting_mediafile in origin_meeting_mediafiles.values():
            new_id = next(new_meeting_mediafile_ids)
            mediafile_id = replace_map[origin_meeting_mediafile["mediafile_id"]]
            new_meeting_mediafiles[new_id] = {
                "id": new_id,
                "is_public": origin_meeting_mediafile["is_public"],
                "mediafile_id": mediafile_id,
                "meeting_id": instance["meeting_id"],
                "meta_new": True,
            }
            origin_mediafile = new_mediafiles[mediafile_id]
            if self.is_orga_wide(origin_mediafile):
                origin_mediafile.get("meeting_mediafile_ids", []).append(new_id)
                origin_meeting_mediafile.get("attachment_ids", []).append(
                    f"motion/{instance['id']}"
                )
            else:
                origin_mediafile["meeting_mediafile_ids"] = [new_id]
                origin_meeting_mediafile["attachment_ids"] = [
                    f"motion/{instance['id']}"
                ]
            instance.setdefault("attachment_meeting_mediafile_ids", []).append(new_id)

        return {
            "mediafile": new_mediafiles,
            "meeting_mediafile": new_meeting_mediafiles,
        }

    def get_history_information(self) -> HistoryInformation | None:
        forwarded_entries = defaultdict(list)
        for instance in self.instances:
            forwarded_entries[
                fqid_from_collection_and_id("motion", instance["origin_id"])
            ].extend(
                [
                    "Forwarded to {}",
                    fqid_from_collection_and_id("meeting", instance["meeting_id"]),
                ]
            )
        return forwarded_entries | {
            fqid_from_collection_and_id("motion", instance["id"]): [
                "Motion created (forwarded)"
            ]
            for instance in self.instances
        }
