import logging
import math
import os
import pickle
import re
import sys
from json import JSONDecoder, JSONEncoder
from typing import Any, Dict, List, Set, Tuple

import neuralcoref
import spacy
from spacy.lang.en import English

sys.path.insert(1, '../eventExtraction')
from utilities import flatten

ETYPE_PATIENT = "Patient"
ETYPE_VISIT = "Visit"
ETYPE_EVENT = "Event"


class Query():
    def __init__(self, query_id, query_question, query_ett_type,
                 query_ett_attr, query_event, query_embedding):
        self.query_id = query_id
        self.query = query_question
        self.patient = '?'
        self.query_entity_attribute = query_ett_attr
        self.query_event = query_event
        self.query_embedding = query_embedding


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
        p = Patient(patient_embedding=obj['entity_embedding'],
                    patient_id=obj['entity_id'],
                    patient_age=obj['age'],
                    patient_dob=obj['dob'],
                    patient_gender=obj['gender'],
                    patient_is_adult=obj['is_adult'],
                    patient_smoker=obj['smoker'])
        p.visits.extend(obj['visits'])
        return p

    @staticmethod
    def decode_visit(obj):
        """Decode a Visit obj."""
        v = Visit(visit_embedding=obj['entity_embedding'],
                  hadm_id=obj['hadm_id'],
                  provenance=obj['provenance'])
        v.events.extend(obj['events'])
        return v

    @staticmethod
    def decode_event(obj):
        """Decode an Event obj."""
        e = Event(event_embedding=obj['entity_embedding'],
                  event_id=obj['entity_id'],
                  chartdate=obj['chartdate'],
                  provenance=obj['provenance'])
        e.event_type = obj['event_type']
        e.roles = obj['roles']
        return e

    @staticmethod
    def decode_entity(obj):
        """Decode an Entity obj."""
        e = Entity(entity_embedding=obj['entity_embedding'],
                   entity_id=obj['entity_id'],
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
        return {
            '__type__': '__Patient__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'entity_embedding': obj.entity_embedding,
            'age': obj.age,
            'dob': obj.dob,
            'gender': obj.gender,
            'is_adult': obj.is_adult,
            'smoker': obj.smoker,
            'visits': [self.default(v) for v in obj.visits]
        }

    def encode_visit(self, obj):
        """Encode a Visit obj."""
        return {
            '__type__': '__Visit__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'entity_embedding': obj.entity_embedding,
            'hadm_id': obj.hadm_id,
            'provenance': obj.provenance,
            'events': [self.default(e) for e in obj.events]
        }

    @staticmethod
    def encode_event(obj):
        """Encode an Event obj."""
        return {
            '__type__': '__Event__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'entity_embedding': obj.entity_embedding,
            'chartdate': obj.chartdate,
            'event_type': obj.event_type,
            'provenance': obj.provenance,
            'roles': obj.roles
        }

    @staticmethod
    def encode_entity(obj):
        """Encode an Entity obj."""
        return {
            '__type__': '__Entity__',
            'entity_type': obj.entity_type,
            'entity_id': obj.entity_id,
            'entity_embedding': obj.entity_embedding
        }


class Entity():
    """Base entity class."""

    def __init__(self, entity_embedding=None, entity_id: str = "",
                 entity_type: str = ""):
        """Initialize the base class."""
        self.entity_embedding = entity_embedding
        self.entity_id: str = entity_id
        self.entity_type: str = entity_type


class Event(Entity):
    """Event class."""

    def __init__(self, event_embedding=None, event_id: str = "",
                 chartdate: str = "", event_type: str = "",
                 provenance: str = ""):
        """Initialize Event."""
        Entity.__init__(self, event_embedding, event_id, ETYPE_EVENT)
        self.chartdate: str = chartdate
        self.event_type: str = event_type
        self.provenance: str = provenance  # refers to parent Visit.hadm_id
        self.roles: Dict = {}
    
    def patient_role(self, attribute: str, attribute_value: Any):
        self.event_type = "PatientEvent"
        self.roles['attribute'] = attribute
        self.roles['attribute_value'] = attribute_value

    def medication_role(self, dosage: str, duration: str,
                        indication: str, medication: str):
        """Medication event helper."""
        self.event_type = "MedicationEvent"
        self.roles['dosage'] = dosage
        self.roles['duration'] = duration
        self.roles['indication'] = indication
        self.roles['medication'] = medication

    def diagnosis_role(self, diagnosis_icd9: str, diagnosis_name: str):
        """Diagnosis event helper."""
        self.event_type = "DiagnosisEvent"
        self.roles['diagnosis_icd9'] = diagnosis_icd9
        self.roles['diagnosis_name'] = diagnosis_name

    def procedure_role(self, procedure_icd9: str, procedure_name: str,
                       targeted_organs: List[str]):
        """Procedure event helper."""
        self.event_type = "ProcedureEvent"
        self.roles['procedure_icd9'] = procedure_icd9
        self.roles['procedure_name'] = procedure_name
        self.roles['targeted_organs'] = targeted_organs

    def lab_role(self, test_name: str, test_value: str, test_status: str):
        """Lab event helper."""
        self.event_type = "LabEvent"
        self.roles['test_name'] = test_name
        self.roles['test_status'] = test_status
        self.roles['test_value'] = test_value

    def vital_role(self, location: str, vital_outcome: str):
        """Vital event helper."""
        self.event_type = "VitalEvent"
        self.roles['location'] = location
        # vital_outcome = ("ALIVE" | "DEAD")
        self.roles['vital_outcome'] = vital_outcome

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


class Visit(Entity):
    """Visit class."""

    def __init__(self, visit_embedding=None, hadm_id: str = "",
                 provenance: str = ""):
        """Initialize Visit."""
        Entity.__init__(self, visit_embedding, hadm_id, ETYPE_VISIT)
        self.hadm_id: str = hadm_id
        # refers to parent Patient.patient_id
        self.provenance: str = provenance
        self.events: List[Event] = []

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


class Patient(Entity):
    """Patient/Pt/Subject class."""

    def __init__(self, patient_embedding=None, patient_id: str = "",
                 patient_age: Any = None, patient_dob: str = "",
                 patient_gender: str = "", patient_is_adult: bool = False,
                 patient_smoker: bool = False):
        """Initialize Patient."""
        Entity.__init__(self, patient_embedding, patient_id, ETYPE_PATIENT)
        self.age: Any = patient_age
        self.dob: str = patient_dob
        self.gender: str = patient_gender
        self.is_adult: bool = patient_is_adult
        self.smoker: bool = patient_smoker
        # TODO, make sure visits are unique
        self.visits: List[Visit] = []

    #FIXME
    def update(self, patient_attributes):
        if patient_attributes.get('age'):
            self.age = patient_attributes['age']
            logging.debug("\tPatient age updated")

        if patient_attributes.get('dob'):
            self.dob = patient_attributes['dob']
            logging.debug("\tPatient dob updated")

        if patient_attributes.get('gender'):
            self.gender = patient_attributes['gender']
            logging.debug("\tPatient gender updated")

        if patient_attributes.get('is_adult'):
            self.is_adult = patient_attributes['is_adult']
            logging.debug("\tPatient is_adult updated")

        if patient_attributes.get('smoker'):
            self.smoker = patient_attributes['smoker']
            logging.debug("\tPatient smoker updated")
    
    def num_visits(self):
        return len(self.visits)

    def __eq__(self, other):
        """Test if Patient objects are equal."""
        if not isinstance(other, Patient):
            # don't attempt to compare against unrelated types
            return NotImplemented

        age_equal = self.age == other.age
        dob_equal = self.dob == other.dob
        gender_equal = self.gender == other.gender
        is_adult_equal = self.is_adult == other.is_adult
        smoker_equal = self.smoker == other.smoker
        visits_equal = sorted(self.visits) == sorted(other.visits)

        patient_equal = age_equal and is_adult_equal and dob_equal and \
            gender_equal and smoker_equal and visits_equal
        return patient_equal
