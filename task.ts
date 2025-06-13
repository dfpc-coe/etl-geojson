import hash from 'object-hash';
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType, InputFeatureCollection } from '@tak-ps/etl';

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

export default class Task extends ETL {
    static name = 'etl-geojson';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Environment
            } else {
                return Type.Object({})
            }
        } else {
            return Type.Object({})
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const url = new URL(env.URL);

        for (const param of env.QueryParams || []) {
            url.searchParams.append(param.key, param.value);
        }

        const headers: Record<string, string> = {};
        for (const header of env.Headers || []) {
            headers[header.key] = header.value;
        }

        const res = await fetch(url, {
            method: 'GET',
            headers
        });

        // TODO: Type the response
        const body: any = await res.json();

        if (body.type !== 'FeatureCollection') {
            throw new Error('Only FeatureCollection is supported');
        }

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        for (const feat of body.features) {
            if (env.RemoveID) delete feat.id;

            if (!feat.geometry) continue;

            fc.features.push({
                id: feat.id || hash(feat),
                type: 'Feature',
                properties: {
                    metadata: feat.properties
                },
                geometry: feat.geometry
            });
        }

        console.log(`ok - obtained ${fc.features.length} features`);

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

