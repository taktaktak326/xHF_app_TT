import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useData } from '../context/DataContext';
import { useLanguage } from '../context/LanguageContext';
import type { Field } from '../types/farm';
import './FarmsPage.css'; // Reuse common styles
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';

const CLUSTER_RADIUS_KM = 2;
const EARTH_RADIUS_KM = 6371;

type FieldCenter = {
  latitude: number;
  longitude: number;
};

type FieldEntry = {
  uuid: string;
  name: string;
  center: FieldCenter;
};

type FieldCluster = {
  id: string;
  fields: FieldEntry[];
};

const getFieldCenter = (field: Field): FieldCenter | null => {
  const candidates = [field.location?.center, (field as any).center, (field as any).centroid];
  for (const candidate of candidates) {
    if (candidate && typeof candidate.latitude === 'number' && typeof candidate.longitude === 'number') {
      return { latitude: candidate.latitude, longitude: candidate.longitude };
    }
  }
  return null;
};

const toRadians = (deg: number): number => (deg * Math.PI) / 180;

const distanceKm = (a: FieldCenter, b: FieldCenter): number => {
  const dLat = toRadians(b.latitude - a.latitude);
  const dLon = toRadians(b.longitude - a.longitude);
  const lat1 = toRadians(a.latitude);
  const lat2 = toRadians(b.latitude);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return EARTH_RADIUS_KM * c;
};

const clusterFieldsByDistance = (fields: FieldEntry[], radiusKm: number): FieldCluster[] => {
  const unassigned = new Set(fields.map(f => f.uuid));
  const lookup = new Map(fields.map(f => [f.uuid, f]));
  const clusters: FieldCluster[] = [];
  let counter = 1;

  for (const field of fields) {
    if (!unassigned.has(field.uuid)) continue;
    const members: FieldEntry[] = [];
    const queue: FieldEntry[] = [field];
    unassigned.delete(field.uuid);

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) continue;
      members.push(current);
      for (const candidateUuid of Array.from(unassigned)) {
        const candidate = lookup.get(candidateUuid);
        if (!candidate) {
          unassigned.delete(candidateUuid);
          continue;
        }
        const within = members.some(member => distanceKm(member.center, candidate.center) <= radiusKm);
        if (within) {
          unassigned.delete(candidateUuid);
          queue.push(candidate);
        }
      }
    }

    clusters.push({ id: `cluster-${counter}`, fields: members });
    counter += 1;
  }

  return clusters;
};

export function WeatherSelectionPage() {
  const {
    combinedOut,
    combinedLoading,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { language, t } = useLanguage();
  const navigate = useNavigate();

  const fields = useMemo(() => {
    return (combinedOut?.response?.data?.fieldsV2 || []) as Field[];
  }, [combinedOut]);
  const { clusters, withoutLocation } = useMemo(() => {
    const withLocation: FieldEntry[] = [];
    const withoutLocation: Field[] = [];
    fields.forEach(field => {
      const center = getFieldCenter(field);
      if (!center) {
        withoutLocation.push(field);
        return;
      }
      withLocation.push({ uuid: field.uuid, name: field.name, center });
    });
    const clusters = clusterFieldsByDistance(withLocation, CLUSTER_RADIUS_KM);
    return { clusters, withoutLocation };
  }, [fields]);

  return (
    <div className="farms-page-container">
      {combinedLoading && (
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            t('weather.selection.loading_label'),
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
      )}
      <h2>{t('weather.selection.title')}</h2>
      <p>{t('weather.selection.description')}</p>

      <div className="field-list-container">
        {clusters.map(cluster => {
          const primary = cluster.fields[0];
          const displayName = cluster.fields.length > 1
            ? t('weather.selection.cluster_display', { name: primary.name, count: cluster.fields.length - 1 })
            : primary.name;
          const tooltip = cluster.fields.map(field => field.name).join(language === 'ja' ? '、' : ', ');
          return (
            <div
              key={cluster.id}
              className="farm-card cluster-card"
              title={tooltip}
              aria-label={tooltip}
              onClick={() =>
                navigate(`/weather/${primary.uuid}`, {
                  state: {
                    clusterId: cluster.id,
                    representativeUuid: primary.uuid,
                    fieldUuids: cluster.fields.map(f => f.uuid),
                    fieldNames: cluster.fields.map(f => f.name),
                    radiusKm: CLUSTER_RADIUS_KM,
                  },
                })
              }
            >
              <h4>{displayName}</h4>
              <p className="cluster-meta">{t('weather.selection.cluster_meta', { count: cluster.fields.length, km: CLUSTER_RADIUS_KM })}</p>
            </div>
          );
        })}
        {withoutLocation.map(field => (
          <div
            key={field.uuid}
            className="farm-card cluster-card cluster-card--solo"
            onClick={() => navigate(`/weather/${field.uuid}`)}
          >
            <h4>{field.name}</h4>
            <p className="cluster-meta">{t('weather.selection.no_location')}</p>
          </div>
        ))}
        {!combinedLoading && fields.length === 0 && (
          <p>{t('weather.selection.no_fields')}</p>
        )}
      </div>
    </div>
  );
}
