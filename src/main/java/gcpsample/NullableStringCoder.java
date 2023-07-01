package gcpsample;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.Coder.Context;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NullableStringCoder extends AtomicCoder<String> {
    private final VarIntCoder nullCoder = VarIntCoder.of(); // Coder for representing null values
    private final StringUtf8Coder stringCoder = StringUtf8Coder.of(); // Coder for non-null string values

    @Override
    public void encode(String value, OutputStream outStream) throws CoderException, IOException {
        if (value == null) {
            nullCoder.encode(0, outStream); // 0 represents null value
        } else {
            nullCoder.encode(1, outStream);
            stringCoder.encode(value, outStream);
        }
    }



    @Override
    public String decode(InputStream inStream) throws CoderException, IOException {
        int isNull = nullCoder.decode(inStream);
        if (isNull == 0) {
            return null;
        } else {
            return stringCoder.decode(inStream);
        }
    }


}

