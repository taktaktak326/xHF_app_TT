import { useEffect, useMemo, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

type FieldLocation = { latitude: number; longitude: number };
type Geometry = { type: 'Polygon' | 'MultiPolygon'; coordinates: any };

const parseGeometry = (boundary: any): Geometry | null => {
  if (!boundary) return null;
  const pickGeometry = (raw: any) => {
    if (!raw || typeof raw !== 'object') return null;
    const candidate = raw.geojson || raw.geoJson || raw.geometry || raw;
    if (
      candidate &&
      (candidate.type === 'Polygon' || candidate.type === 'MultiPolygon') &&
      Array.isArray(candidate.coordinates)
    ) {
      return { type: candidate.type, coordinates: candidate.coordinates } as Geometry;
    }
    return null;
  };

  if (typeof boundary === 'string') {
    const text = boundary.trim();
    if (text.startsWith('{') && text.endsWith('}')) {
      try {
        const parsed = JSON.parse(text);
        return pickGeometry(parsed);
      } catch {
        return null;
      }
    }
    return null;
  }
  return pickGeometry(boundary);
};

const getFieldCenter = (field: any): FieldLocation | null => {
  const candidates = [field?.location?.center, field?.center, field?.centroid];
  for (const cand of candidates) {
    if (cand && typeof cand.latitude === 'number' && typeof cand.longitude === 'number') {
      return { latitude: cand.latitude, longitude: cand.longitude };
    }
  }
  return null;
};

const toFeature = (field: any, key: string | number) => {
  const geometry = parseGeometry(field?.boundary);
  if (geometry) {
    return {
      type: 'Feature',
      properties: {
        name: field?.name,
        uuid: field?.uuid,
        __fieldKey: key,
      },
      geometry,
    };
  }
  const center = getFieldCenter(field);
  if (center) {
    return {
      type: 'Feature',
      properties: {
        name: field?.name,
        uuid: field?.uuid,
        __fieldKey: key,
      },
      geometry: {
        type: 'Point',
        coordinates: [center.longitude, center.latitude],
      },
    };
  }
  return null;
};

export function FieldMap({ fields, onFieldClick }: { fields: any[]; onFieldClick?: (field: any) => void }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);
  const fieldsMapRef = useRef<Map<string | number, any>>(new Map());

  const keyedFields = useMemo(() => {
    const map = new Map<string | number, any>();
    const list = fields.map((f, idx) => {
      const key = f?.uuid ?? idx;
      map.set(key, f);
      return { key, field: f };
    });
    fieldsMapRef.current = map;
    return list;
  }, [fields]);

  useEffect(() => {
    if (mapRef.current || !containerRef.current) return;
    const map = L.map(containerRef.current, {
      center: [36, 138],
      zoom: 5,
      worldCopyJump: true,
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap contributors',
    }).addTo(map);
    mapRef.current = map;
    layerRef.current = L.layerGroup().addTo(map);
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const layerGroup = layerRef.current;
    if (!map || !layerGroup) return;

    layerGroup.clearLayers();

    let bounds: L.LatLngBounds | null = null;

    keyedFields.forEach(({ field, key }) => {
      const feature = toFeature(field, key);
      if (!feature) return;
      const geoLayer = L.geoJSON(feature as any, {
        style: {
          color: '#8ab4ff',
          weight: 2,
          fillColor: '#5c6ac4',
          fillOpacity: 0.15,
        },
        onEachFeature: (feat, layer) => {
          layer.on('click', () => {
            const fkey = (feat.properties as any)?.__fieldKey ?? key;
            const target = fieldsMapRef.current.get(fkey) || field;
            onFieldClick?.(target);
          });
        },
      });
      geoLayer.addTo(layerGroup);
      if (geoLayer.getBounds && geoLayer.getBounds().isValid()) {
        const b = geoLayer.getBounds();
        bounds = bounds ? bounds.extend(b) : b;
      }
      const center = getFieldCenter(field);
      if (center) {
        const marker = L.circleMarker([center.latitude, center.longitude], {
          radius: 4,
          color: '#ffb74d',
          weight: 2,
          fillColor: '#ff9800',
          fillOpacity: 0.9,
        });
        marker.bindTooltip(field?.name ?? '圃場', { direction: 'top' });
        marker.addTo(layerGroup);
      }
    });

    const finalBounds = bounds as L.LatLngBounds | null;
    if (finalBounds && finalBounds.isValid()) {
      map.fitBounds(finalBounds.pad(0.15));
    } else {
      map.setView([36, 138], 5);
    }
  }, [fields]);

  return <div ref={containerRef} className="satellite-leaflet" />;
}
