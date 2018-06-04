/*
 * Licensed under the Rakam Incorporation
 */

package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

import java.io.IOException;

public class GenericDatumReader0<D>
        extends GenericDatumReader<D>
{
    public GenericDatumReader0(Schema writer, Schema reader)
    {
        super(writer, reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public D read(D reuse, Decoder in)
            throws IOException
    {
        ResolvingDecoder resolver = getResolver(getSchema(), getExpected());
        resolver.configure(in);
        D result = (D) read(reuse, getExpected(), resolver);
// Do not drain the record because the number of columns in this record may be less than the schema so it can fail,
// also there is no benefit of draining as this is a single record
//        resolver.drain();
        return result;
    }
}
