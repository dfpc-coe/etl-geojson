import hash from 'object-hash';
import { Feature } from '@tak-ps/node-cot';
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const Environment = Type.Object({
    URL: Type.String(),
    QueryParams: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    Headers: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    RemoveID: Type.Boolean({
        default: false,
        description: 'Remove the provided ID falling back to an Object Hash or Style Override'
    })
});

type InputFeature = Static<typeof Feature.InputFeature>;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isGeometry(value: Record<string, unknown>): value is InputFeature['geometry'] {
    return typeof value.type === 'string';
}

function featureId(id: unknown): string {
    if (typeof id === 'string') return id;
    if (typeof id === 'number' && id !== 0) return String(id);
    return '';
}

export default class Task extends ETL {
    static name = 'etl-geojson';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming && type === SchemaType.Input) return Environment;

        return Type.Object({});
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const url = new URL(env.URL);

        for (const param of env.QueryParams ?? []) {
            url.searchParams.append(param.key, param.value);
        }

        const headers: Record<string, string> = {};
        for (const header of env.Headers ?? []) {
            headers[header.key] = header.value;
        }

        const res = await fetch(url, {
            method: 'GET',
            headers
        });

        const body: unknown = await res.json();

        if (!isObject(body) || body.type !== 'FeatureCollection') {
            throw new Error('Only FeatureCollection is supported');
        }

        if (!Array.isArray(body.features)) throw new TypeError('FeatureCollection is missing features');
        const features: unknown[] = body.features;

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        for (const feat of features) {
            if (!isObject(feat)) continue;

            if (env.RemoveID) delete feat.id;

            const { geometry } = feat;
            if (!isObject(geometry) || !isGeometry(geometry)) continue;

            const id = featureId(feat.id);
            const metadata = isObject(feat.properties) ? feat.properties : undefined;

            if (geometry.type.startsWith('Multi')) {
                const type = geometry.type.replace('Multi', '');

                geometry.coordinates.forEach((coordinates, idx) => {
                    const part: Record<string, unknown> = { type, coordinates };
                    if (!isGeometry(part)) return;

                    fc.features.push({
                        id: id ? `${id}-${idx}` : hash(coordinates),
                        type: 'Feature',
                        properties: { metadata },
                        geometry: part
                    });
                });
            } else {
                fc.features.push({
                    id: id || hash(feat),
                    type: 'Feature',
                    properties: { metadata },
                    geometry
                });
            }
        }

        console.log(`ok - obtained ${fc.features.length} features`);

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}

