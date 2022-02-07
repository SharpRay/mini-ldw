package com.mininglamp.io;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Any class that requires persistence should implement the Writable interface.
 * This interface requires only a uniform writable method "write()",
 * but does not require a uniform read method.
 * The usage of writable interface implementation class is as follows:
 *
 * Class A implements Writable {
 *      @Override
 *      public void write(DataOutput out) throws IOException {
 *          in.write(x);
 *          in.write(y);
 *          ...
 *      }
 *
 *      private void readFields(DataInput in) throws IOException {
 *          x = in.read();
 *          y = in.read();
 *          ...
 *      }
 *
 *      public static A read(DataInput in) throws IOException {
 *          A a = new A();
 *          a.readFields();
 *          return a;
 *      }
 * }
 *
 * A a = new A();
 * a.write(out);
 * ...
 * A other = A.read(in);
 *
 * The "readFields()" can be implemented as whatever you like, or even without it
 * by just implementing the static read method.
 */
public interface Writable {
    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOutput</code> to serialize this object into.
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;
}
