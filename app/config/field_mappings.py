from dataclasses import dataclass
from typing import Dict, Any
from enum import Enum


class ServiceType(str, Enum):
    AUTHORITATIVE = "authoritative"
    EVENTUAL = "eventual"


class RecordType(str, Enum):
    ACTIVITY = "activity"
    CENTRE_ACTIVITY = "centre_activity"
    CENTRE_ACTIVITY_PREFERENCE = "centre_activity_preference"
    CENTRE_ACTIVITY_RECOMMENDATION = "centre_activity_recommendation"
    CENTRE_ACTIVITY_EXCLUSION = "centre_activity_exclusion"
    PATIENT = "patient"
    PATIENT_MEDICATION = "patient_medication"


@dataclass
class ServiceMapping:
    """Configuration for mapping between authoritative and eventual services"""
    record_type: RecordType
    authoritative_endpoint: str
    eventual_endpoint: str
    id_field_mapping: Dict[str, str]  # eventual_field -> authoritative_field
    timestamp_field_mapping: Dict[str, str]  # eventual_field -> authoritative_field
    additional_filters: Dict[str, Any] = None


# Field mappings configuration
FIELD_MAPPINGS = {
    RecordType.ACTIVITY: ServiceMapping(
        record_type=RecordType.ACTIVITY,
        authoritative_endpoint="/integrity/activity",
        eventual_endpoint="/integrity/ref-activity",
        id_field_mapping={"ActivityID": "id"},
        timestamp_field_mapping={"UpdatedDateTime": "modified_date"}
    ),
    
    RecordType.CENTRE_ACTIVITY: ServiceMapping(
        record_type=RecordType.CENTRE_ACTIVITY,
        authoritative_endpoint="/integrity/centre-activity", 
        eventual_endpoint="/integrity/ref-centre-activity",
        id_field_mapping={"CentreActivityID": "id", "ActivityID": "activity_id"},
        timestamp_field_mapping={"UpdatedDateTime": "modified_date"}
    ),
    
    RecordType.CENTRE_ACTIVITY_PREFERENCE: ServiceMapping(
        record_type=RecordType.CENTRE_ACTIVITY_PREFERENCE,
        authoritative_endpoint="/integrity/centre-activity-preference",
        eventual_endpoint="/integrity/ref-centre-activity-preference", 
        id_field_mapping={"PreferenceID": "id", "CentreActivityID": "centre_activity_id", "PatientID": "patient_id"},
        timestamp_field_mapping={"UpdatedDateTime": "modified_date"}
    ),
    
    RecordType.CENTRE_ACTIVITY_RECOMMENDATION: ServiceMapping(
        record_type=RecordType.CENTRE_ACTIVITY_RECOMMENDATION,
        authoritative_endpoint="/integrity/centre-activity-recommendation",
        eventual_endpoint="/integrity/ref-centre-activity-recommendation",
        id_field_mapping={"RecommendationID": "id", "CentreActivityID": "centre_activity_id", "PatientID": "patient_id", "DoctorID": "doctor_id"},
        timestamp_field_mapping={"UpdatedDateTime": "modified_date"}
    ),
    
    RecordType.CENTRE_ACTIVITY_EXCLUSION: ServiceMapping(
        record_type=RecordType.CENTRE_ACTIVITY_EXCLUSION,
        authoritative_endpoint="/integrity/centre-activity-exclusion",
        eventual_endpoint="/integrity/ref-centre-activity-exclusion",
        id_field_mapping={"ActivityExclusionID": "id", "ActivityID": "centre_activity_id", "PatientID": "patient_id"},
        timestamp_field_mapping={"modified_date": "modified_date"}
    ),
    
    RecordType.PATIENT: ServiceMapping(
        record_type=RecordType.PATIENT,
        authoritative_endpoint="/integrity/patient",
        eventual_endpoint="/integrity/ref-patient",
        id_field_mapping={"PatientID": "id"},
        timestamp_field_mapping={"modified_date": "modified_date"}
    ),
    
    RecordType.PATIENT_MEDICATION: ServiceMapping(
        record_type=RecordType.PATIENT_MEDICATION,
        authoritative_endpoint="/integrity/patient-medication",
        eventual_endpoint="/integrity/ref-patient-medication",
        id_field_mapping={"MedicationID": "Id", "PatientID": "PatientId"},
        timestamp_field_mapping={"UpdatedDateTime": "UpdatedDateTime"}
    )
}


def get_service_configs():
    """Get service configuration with URLs from settings"""
    from app.config.settings import get_settings
    settings = get_settings()
    
    return {
        "activity": {
            "base_url": settings.ACTIVITY_SERVICE_URL,
            "service_type": ServiceType.AUTHORITATIVE,
            "timeout": settings.RECONCILIATION_TIMEOUT_SECONDS
        },
        "patient": {
            "base_url": settings.PATIENT_SERVICE_URL, 
            "service_type": ServiceType.AUTHORITATIVE,
            "timeout": settings.RECONCILIATION_TIMEOUT_SECONDS
        },
        "scheduler": {
            "base_url": settings.SCHEDULER_SERVICE_URL,
            "service_type": ServiceType.EVENTUAL,
            "timeout": settings.RECONCILIATION_TIMEOUT_SECONDS
        }
    }


def get_authoritative_service_for_record_type(record_type: RecordType) -> str:
    """Determine which service is authoritative for a given record type"""
    if record_type in [
        RecordType.ACTIVITY, 
        RecordType.CENTRE_ACTIVITY,
        RecordType.CENTRE_ACTIVITY_PREFERENCE, 
        RecordType.CENTRE_ACTIVITY_RECOMMENDATION,
        RecordType.CENTRE_ACTIVITY_EXCLUSION
    ]:
        return "activity"
    elif record_type in [RecordType.PATIENT, RecordType.PATIENT_MEDICATION]:
        return "patient"
    else:
        raise ValueError(f"Unknown record type: {record_type}")


def get_eventual_service_for_record_type(record_type: RecordType) -> str:
    """Determine which service has eventual consistent data for a given record type"""
    # For now, all eventual consistent data is in the scheduler service
    return "scheduler"


def get_primary_id_field(record_type: RecordType) -> str:
    """Get the primary ID field name for a record type (in authoritative format)"""
    mapping = FIELD_MAPPINGS.get(record_type)
    if not mapping:
        raise ValueError(f"No mapping found for record type: {record_type}")
    
    # Return the first (primary) ID field in authoritative format
    return list(mapping.id_field_mapping.values())[0]


def transform_eventual_to_authoritative_format(
    record: Dict[str, Any], 
    mapping: ServiceMapping
) -> Dict[str, Any]:
    """Transform a record from eventual service format to authoritative format"""
    transformed = {}
    
    # Map ID fields
    for eventual_field, auth_field in mapping.id_field_mapping.items():
        if eventual_field in record:
            transformed[auth_field] = record[eventual_field]
    
    # Map timestamp fields
    for eventual_field, auth_field in mapping.timestamp_field_mapping.items():
        if eventual_field in record:
            transformed[auth_field] = record[eventual_field]
    
    # Copy other standard fields
    for field in ["record_type", "modified_date", "version_timestamp"]:
        if field in record and field not in transformed:
            transformed[field] = record[field]
    
    return transformed
