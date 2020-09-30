import logging
import math
import os
import re
import sys
from datetime import date, datetime
from json import JSONDecoder, JSONEncoder
from typing import Any, Dict, List, Optional, Set, Tuple

from backports.datetime_fromisoformat import MonkeyPatch

MonkeyPatch.patch_fromisoformat()

ETYPE_PATIENT = "Patient"
ETYPE_VISIT = "Visit"
ETYPE_EVENT = "Event"


class EntityDecoder(JSONDecoder):
    """Decoder for Entities."""

    def __init__(self, *args, **kwargs):
        """Initialize Entity Decoder."""
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args,
                             **kwargs)

    def object_hook(self, obj):
        """Decode based on object type."""
        if '__type__' in obj:
            if obj['__type__'] == '__Patient__':
                return self.decode_patient(obj)
            elif obj['__type__'] == '__Visit__':
                return self.decode_visit(obj)
            elif obj['__type__'] == '__Event__':
                return self.decode_event(obj)
            elif obj['__type__'] == '__Entity__':
                return self.decode_entity(obj)
        return obj

    @staticmethod
    def decode_patient(obj):
        """Decode a Patient obj."""
        try:
            date_of_birth_obj = date.fromisoformat(obj['date_of_birth'])
        except ValueError:
            date_of_birth_obj = None
        p = Patient(patient_id=obj['patient_id'],
                    patient_adult=obj['adult'],
                    patient_age=obj['age'],
                    patient_date_of_birth=date_of_birth_obj,
                    patient_ethnicity=obj['ethnicity'],
                    patient_gender=obj['gender'],
                    patient_race=obj['race'],
                    patient_smoker=obj['smoker'])
        p.visits.extend(obj['visits'])
        return p

    @staticmethod
    def decode_visit(obj):
        """Decode a Visit obj."""
        date_obj = datetime.fromisoformat(obj['date'])
        v = Visit(visit_id=obj['visit_id'],
                  patient_id=obj['patient_id'],
                  date=date_obj)
        v.events.extend(obj['events'])
        return v

    @staticmethod
    def decode_event(obj):
        """Decode an Event obj."""
        e = Event(event_id=obj['entity_id'],
                  visit_id=obj['visit_id'],
                  patient_id=obj['patient_id'],
                  chartdate=obj['chartdate'],
                  event_type=obj['event_type'])
        e.roles = obj['roles']
        return e

    @staticmethod
    def decode_entity(obj):
        """Decode an Entity obj."""
        e = Entity(entity_id=obj['entity_id'],
                   entity_type=obj['entity_type'])
        return e


class EntityEncoder(JSONEncoder):
    """Encoder for Entities."""

    def default(self, obj):  # pylint: disable=E0202
        """Encode based on object type."""
        if isinstance(obj, Patient):
            return self.encode_patient(obj)
        elif isinstance(obj, Visit):
            return self.encode_visit(obj)
        elif isinstance(obj, Event):
            return self.encode_event(obj)
        elif isinstance(obj, Entity):
            return self.encode_entity(obj)
        else:
            return JSONEncoder.default(self, obj)

    def encode_patient(self, obj):
        """Encode a Patient obj."""
        try:
            date_of_birth_str = obj.date_of_birth.isoformat()
        except AttributeError:
            date_of_birth_str = ''
        return {
            '__type__': '__Patient__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'patient_id': obj.patient_id,
            'adult': obj.adult,
            'age': obj.age,
            'date_of_birth': date_of_birth_str,
            'ethnicity': obj.ethnicity,
            'gender': obj.gender,
            'race': obj.race,
            'smoker': obj.smoker,
            'visits': [self.default(v) for v in obj.visits]
        }

    def encode_visit(self, obj):
        """Encode a Visit obj."""
        return {
            '__type__': '__Visit__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'visit_id': obj.visit_id,
            'date': obj.date.isoformat(),
            'patient_id': obj.patient_id,
            'events': [self.default(e) for e in obj.events]
        }

    @staticmethod
    def encode_event(obj):
        """Encode an Event obj."""
        return {
            '__type__': '__Event__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'patient_id': obj.patient_id,
            'visit_id': obj.visit_id,
            'event_id': obj.event_id,
            'chartdate': obj.chartdate,
            'event_type': obj.event_type,
            'roles': obj.roles
        }

    @staticmethod
    def encode_entity(obj):
        """Encode an Entity obj."""
        return {
            '__type__': '__Entity__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
        }


class Entity():
    """Base entity class."""

    def __init__(self,
                 entity_id: str = "",
                 entity_type: str = ""):
        """Initialize the base class."""
        # This entity id should only be changed by PatientDB
        self.entity_id: str = entity_id
        self.entity_type: str = entity_type


class Event(Entity):
    """Event class."""

    def __init__(self,
                 event_id: str = "",
                 visit_id: str = "",
                 patient_id: str = "",
                 chartdate: str = "",
                 event_type: str = ""):
        """Initialize Event."""
        Entity.__init__(self, entity_type=ETYPE_EVENT)
        # IDs
        self.event_id: str = event_id
        self.visit_id: str = visit_id
        self.patient_id: str = patient_id

        # TODO: make chartdate a date object
        self.chartdate: str = chartdate
        self.event_type: str = event_type
        self.roles: Dict = {}
        # TODO add metadata source attributes

    # FIXME, what is this used for?
    def patient_role(self,
                     attribute: str,
                     attribute_value: Any):
        self.event_type = "PatientEvent"
        self.roles['attribute'] = attribute
        self.roles['attribute_value'] = attribute_value

    def medication_role(self,
                        dosage: str = '',
                        duration: str = '',
                        indication: str = '',
                        medication: str = ''):
        """Medication event helper."""
        self.event_type = "MedicationEvent"
        self.roles['dosage'] = dosage
        self.roles['duration'] = duration
        self.roles['indication'] = indication
        self.roles['medication'] = medication

    def diagnosis_role(self,
                       diagnosis_icd9: str = '',
                       diagnosis_name: str = '',
                       diagnosis_long_name: str = ''):
        """Diagnosis event helper."""
        self.event_type = "DiagnosisEvent"
        self.roles['diagnosis_icd9'] = diagnosis_icd9
        self.roles['diagnosis_name'] = diagnosis_name
        self.roles['diagnosis_long_name'] = diagnosis_long_name

    def procedure_role(self,
                       procedure_icd9: str = '',
                       procedure_name: str = '',
                       targeted_organs: List[str] = None):
        """Procedure event helper."""
        #FIXME, more elegant way to handle this?
        if not targeted_organs:
            targeted_organs = []
        self.event_type = "ProcedureEvent"
        self.roles['procedure_icd9'] = procedure_icd9
        self.roles['procedure_name'] = procedure_name
        self.roles['targeted_organs'] = targeted_organs

    def lab_role(self,
                 test_name: str = '',
                 test_value: str = '',
                 test_status: str = ''):
        """Lab event helper."""
        self.event_type = "LabEvent"
        self.roles['test_name'] = test_name
        self.roles['test_status'] = test_status
        self.roles['test_value'] = test_value

    def vital_role(self,
                   location: str = '',
                   vital_outcome: str = ''):
        """Vital event helper."""
        self.event_type = "VitalEvent"
        self.roles['location'] = location
        # vital_outcome = ("ALIVE" | "DEAD")
        self.roles['vital_outcome'] = vital_outcome

    def meddra_role(self, row):
        self.event_type = "MEDDRAEvent"
        self.add_meddra_roles(row)

    def condition_occurence_role(self, row):
        self.event_type = "CONDITION_OCCURENCE"
        self.add_condition_occurence_roles(row)

    def add_meddra_roles(self, row):
        # Meddra levels
        self.roles['SOC'] = row.SOC
        self.roles['HLGT'] = row.HLGT
        self.roles['HLT'] = row.HLT
        self.roles['PT'] = row.PT

        # Meddra CUI levels
        self.roles['SOC_CUI'] = row.SOC_CUI
        self.roles['HLGT_CUI'] = row.HLGT_CUI
        self.roles['HLT_CUI'] = row.HLT_CUI
        self.roles['PT_CUI'] = row.PT_CUI
        self.roles['extracted_CUI'] = row.extracted_CUI

        # Meddra text levels
        self.roles['SOC_text'] = row.SOC_text
        self.roles['HLGT_text'] = row.HLGT_text
        self.roles['HLT_text'] = row.HLT_text
        self.roles['PT_text'] = row.PT_text
        self.roles['concept_text'] = row.concept_text

        # Everything else (not adding date twice)
        self.roles['PExperiencer'] = row.PExperiencer
        self.roles['medID'] = row.medID
        self.roles['note_id'] = row.note_id
        self.roles['note_title'] = row.note_title
        self.roles['polarity'] = row.polarity
        self.roles['pos'] = row.pos
        self.roles['present'] = row.present
        self.roles['ttype'] = row.ttype

    def add_condition_occurence_roles(self, row):
        self.roles['condition_occurrence_id'] = row.condition_occurence_id
        self.roles['person_id'] = row.person_id
        self.roles['condition_concept_id'] = row.condition_concept_id
        self.roles['condition_start_date'] = row.condition_start_date
        self.roles['condition_start_datetime'] = row.condition_start_datetime
        self.roles['condition_end_date'] = row.condition_end_date
        self.roles['condition_end_datetime'] = row.condition_end_datetime
        self.roles['condition_type_concept_id'] = row.condition_type_concept_id

    # broken FIXME
    def __eq__(self, other):
        """Test if Event objects are equal."""
        if not isinstance(other, Event):
            # don't attempt to compare against unrelated types
            return NotImplemented
        chartdate_equal = self.chartdate == other.chartdate
        event_type_equal = self.event_type == other.event_type
        provenance_equal = self.provenance == other.provenance
        roles_equal = self.roles == other.roles

        event_equal = chartdate_equal and event_type_equal and \
            provenance_equal and roles_equal
        return event_equal

    def __str__(self, sep="  ", indent1=4, indent2=5):
        sep_1 = f"{sep*indent1}"
        sep_2 = f"{sep*indent2}"
        event_str = f"{sep_1}Event {'{'}\n"
        event_str += f"{sep_2}entity_id: {self.entity_id}\n"
        event_str += f"{sep_2}event_id: {self.event_id}\n"
        event_str += f"{sep_2}visit_id: {self.visit_id}\n"
        event_str += f"{sep_2}patient_id: {self.patient_id}\n"
        event_str += f"{sep_2}chartdate: {self.chartdate}\n"
        event_str += f"{sep_2}event_type: {self.event_type}\n"
        event_str += f"{sep_2}roles: {'{'}\n"
        for role, role_value in self.roles.items():
            event_str += f"{sep_2}{sep}{role}: {role_value}\n"
        event_str += f"{sep_2}{'}'}\n"
        event_str += f"{sep_1}{'}'}\n"
        return event_str


class Visit(Entity):
    """Visit class."""

    def __init__(self,
                 date: str = None,
                 visit_id: str = "",
                 patient_id: str = ""):
        """Initialize Visit."""
        Entity.__init__(self, entity_type=ETYPE_VISIT)
        self.visit_id: str = visit_id
        self.patient_id: str = patient_id
        # TODO: make sure date is a datetime obj
        self.date = date
        self.events: List[Event] = []

    def num_events(self):
        num_events = 0
        for event in self.events:
            num_events += 1
        return num_events

    # FIXME, broken
    def __eq__(self, other):
        """Test if Visit objects are equal."""
        if not isinstance(other, Visit):
            # don't attempt to compare against unrelated types
            return NotImplemented
        hadm_id_equal = self.hadm_id == other.hadm_id
        provenance_equal = self.provenance == other.provenance
        events_equal = sorted(self.events) == sorted(other.events)

        visit_equal = hadm_id_equal and provenance_equal and events_equal
        return visit_equal

    def __str__(self, sep="  ", indent1=2, indent2=3):
        sep_1 = f"{sep*indent1}"
        sep_2 = f"{sep*indent2}"
        visit_str = f"{sep_1}Visit {'{'}\n"
        visit_str += f"{sep_2}entity_id: {self.entity_id}\n"
        visit_str += f"{sep_2}visit_id: {self.visit_id}\n"
        visit_str += f"{sep_2}patient_id: {self.patient_id}\n"
        visit_str += f"{sep_2}events: [\n"
        for event in self.events:
            visit_str += str(event)
        visit_str += f"{sep_2}]\n"
        visit_str += f"{sep_1}{'}'}\n"
        return visit_str


class Patient(Entity):
    """Patient/Pt/Subject class."""

    def __init__(self,
                 patient_id: str = "",
                 patient_age: Any = None,
                 patient_date_of_birth: date = None,
                 patient_ethnicity: str = "",
                 patient_gender: str = "",
                 patient_race: str = "",
                 patient_adult: bool = False,
                 patient_smoker: bool = False):
        """Initialize Patient."""
        Entity.__init__(self, entity_type=ETYPE_PATIENT)
        self.patient_id: str = patient_id
        self.adult: bool = patient_adult
        self.age: Any = patient_age
        self.date_of_birth: Optional[date] = patient_date_of_birth
        self.ethnicity: str = patient_ethnicity
        self.gender: str = patient_gender
        self.race: str = patient_race
        self.smoker: bool = patient_smoker
        #self.birth_datetime = None

        # TODO, make sure visits are unique
        self.visits: List[Visit] = []
        # TODO, think about how to incorprate item not associated with Visits
        # or Events
        # e.g. prescription management, maintenance items

    def num_visits(self):
        return len(self.visits)

    def num_events(self):
        num_events = 0
        for visit in self.visits:
            num_events += visit.num_events()

        return num_events

    def get_visit_by_id(self, visit_id: str) -> Visit:
        v = None
        for visit in self.visits:
            if visit.visit_id == visit_id:
                v = visit
        return v


    def __eq__(self, other):
        """Test if Patient objects are equal."""
        if not isinstance(other, Patient):
            # don't attempt to compare against unrelated types
            return NotImplemented

        age_equal = self.age == other.age
        date_of_birth_equal = self.date_of_birth == other.date_of_birth
        gender_equal = self.gender == other.gender
        adult_equal = self.adult == other.adult
        smoker_equal = self.smoker == other.smoker
        visits_equal = sorted(self.visits) == sorted(other.visits)

        patient_equal = age_equal and adult_equal and date_of_birth_equal and \
            gender_equal and smoker_equal and visits_equal
        return patient_equal

    def __str__(self, sep="  ", indent1=0, indent2=1):
        sep_1 = f"{sep * indent1}"
        sep_2 = f"{sep * indent2}"
        patient_str = f"{sep_1}Patient {'{'}\n"
        patient_str += f"{sep_2}entity_id: {self.entity_id}\n"
        patient_str += f"{sep_2}adult: {self.adult}\n"
        patient_str += f"{sep_2}age: {self.age}\n"
        patient_str += f"{sep_2}date_of_birth: {self.date_of_birth}\n"
        patient_str += f"{sep_2}ethnicity: {self.ethnicity}\n"
        patient_str += f"{sep_2}gender: {self.gender}\n"
        patient_str += f"{sep_2}race: {self.race}\n"
        patient_str += f"{sep_2}smoker: {self.smoker}\n"
        patient_str += f"{sep_2}visits: [\n"

        for visit in self.visits:
            patient_str += str(visit)
        patient_str += f"{sep_2}]\n"
        patient_str += f"{sep_1}{'}'}"
        return patient_str
