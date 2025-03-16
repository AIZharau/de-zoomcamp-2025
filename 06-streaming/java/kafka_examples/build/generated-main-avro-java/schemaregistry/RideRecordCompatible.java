/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package schemaregistry;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class RideRecordCompatible extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 7163300507090021229L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RideRecordCompatible\",\"namespace\":\"schemaregistry\",\"fields\":[{\"name\":\"vendorId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"passenger_count\",\"type\":\"int\"},{\"name\":\"trip_distance\",\"type\":\"double\"},{\"name\":\"pu_location_id\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<RideRecordCompatible> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<RideRecordCompatible> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<RideRecordCompatible> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<RideRecordCompatible> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<RideRecordCompatible> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this RideRecordCompatible to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a RideRecordCompatible from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a RideRecordCompatible instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static RideRecordCompatible fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.String vendorId;
  private int passenger_count;
  private double trip_distance;
  private java.lang.Long pu_location_id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RideRecordCompatible() {}

  /**
   * All-args constructor.
   * @param vendorId The new value for vendorId
   * @param passenger_count The new value for passenger_count
   * @param trip_distance The new value for trip_distance
   * @param pu_location_id The new value for pu_location_id
   */
  public RideRecordCompatible(java.lang.String vendorId, java.lang.Integer passenger_count, java.lang.Double trip_distance, java.lang.Long pu_location_id) {
    this.vendorId = vendorId;
    this.passenger_count = passenger_count;
    this.trip_distance = trip_distance;
    this.pu_location_id = pu_location_id;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return vendorId;
    case 1: return passenger_count;
    case 2: return trip_distance;
    case 3: return pu_location_id;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: vendorId = value$ != null ? value$.toString() : null; break;
    case 1: passenger_count = (java.lang.Integer)value$; break;
    case 2: trip_distance = (java.lang.Double)value$; break;
    case 3: pu_location_id = (java.lang.Long)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'vendorId' field.
   * @return The value of the 'vendorId' field.
   */
  public java.lang.String getVendorId() {
    return vendorId;
  }


  /**
   * Sets the value of the 'vendorId' field.
   * @param value the value to set.
   */
  public void setVendorId(java.lang.String value) {
    this.vendorId = value;
  }

  /**
   * Gets the value of the 'passenger_count' field.
   * @return The value of the 'passenger_count' field.
   */
  public int getPassengerCount() {
    return passenger_count;
  }


  /**
   * Sets the value of the 'passenger_count' field.
   * @param value the value to set.
   */
  public void setPassengerCount(int value) {
    this.passenger_count = value;
  }

  /**
   * Gets the value of the 'trip_distance' field.
   * @return The value of the 'trip_distance' field.
   */
  public double getTripDistance() {
    return trip_distance;
  }


  /**
   * Sets the value of the 'trip_distance' field.
   * @param value the value to set.
   */
  public void setTripDistance(double value) {
    this.trip_distance = value;
  }

  /**
   * Gets the value of the 'pu_location_id' field.
   * @return The value of the 'pu_location_id' field.
   */
  public java.lang.Long getPuLocationId() {
    return pu_location_id;
  }


  /**
   * Sets the value of the 'pu_location_id' field.
   * @param value the value to set.
   */
  public void setPuLocationId(java.lang.Long value) {
    this.pu_location_id = value;
  }

  /**
   * Creates a new RideRecordCompatible RecordBuilder.
   * @return A new RideRecordCompatible RecordBuilder
   */
  public static schemaregistry.RideRecordCompatible.Builder newBuilder() {
    return new schemaregistry.RideRecordCompatible.Builder();
  }

  /**
   * Creates a new RideRecordCompatible RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RideRecordCompatible RecordBuilder
   */
  public static schemaregistry.RideRecordCompatible.Builder newBuilder(schemaregistry.RideRecordCompatible.Builder other) {
    if (other == null) {
      return new schemaregistry.RideRecordCompatible.Builder();
    } else {
      return new schemaregistry.RideRecordCompatible.Builder(other);
    }
  }

  /**
   * Creates a new RideRecordCompatible RecordBuilder by copying an existing RideRecordCompatible instance.
   * @param other The existing instance to copy.
   * @return A new RideRecordCompatible RecordBuilder
   */
  public static schemaregistry.RideRecordCompatible.Builder newBuilder(schemaregistry.RideRecordCompatible other) {
    if (other == null) {
      return new schemaregistry.RideRecordCompatible.Builder();
    } else {
      return new schemaregistry.RideRecordCompatible.Builder(other);
    }
  }

  /**
   * RecordBuilder for RideRecordCompatible instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RideRecordCompatible>
    implements org.apache.avro.data.RecordBuilder<RideRecordCompatible> {

    private java.lang.String vendorId;
    private int passenger_count;
    private double trip_distance;
    private java.lang.Long pu_location_id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(schemaregistry.RideRecordCompatible.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.vendorId)) {
        this.vendorId = data().deepCopy(fields()[0].schema(), other.vendorId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.passenger_count)) {
        this.passenger_count = data().deepCopy(fields()[1].schema(), other.passenger_count);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.trip_distance)) {
        this.trip_distance = data().deepCopy(fields()[2].schema(), other.trip_distance);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.pu_location_id)) {
        this.pu_location_id = data().deepCopy(fields()[3].schema(), other.pu_location_id);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing RideRecordCompatible instance
     * @param other The existing instance to copy.
     */
    private Builder(schemaregistry.RideRecordCompatible other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.vendorId)) {
        this.vendorId = data().deepCopy(fields()[0].schema(), other.vendorId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.passenger_count)) {
        this.passenger_count = data().deepCopy(fields()[1].schema(), other.passenger_count);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.trip_distance)) {
        this.trip_distance = data().deepCopy(fields()[2].schema(), other.trip_distance);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.pu_location_id)) {
        this.pu_location_id = data().deepCopy(fields()[3].schema(), other.pu_location_id);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'vendorId' field.
      * @return The value.
      */
    public java.lang.String getVendorId() {
      return vendorId;
    }


    /**
      * Sets the value of the 'vendorId' field.
      * @param value The value of 'vendorId'.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder setVendorId(java.lang.String value) {
      validate(fields()[0], value);
      this.vendorId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'vendorId' field has been set.
      * @return True if the 'vendorId' field has been set, false otherwise.
      */
    public boolean hasVendorId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'vendorId' field.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder clearVendorId() {
      vendorId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'passenger_count' field.
      * @return The value.
      */
    public int getPassengerCount() {
      return passenger_count;
    }


    /**
      * Sets the value of the 'passenger_count' field.
      * @param value The value of 'passenger_count'.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder setPassengerCount(int value) {
      validate(fields()[1], value);
      this.passenger_count = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'passenger_count' field has been set.
      * @return True if the 'passenger_count' field has been set, false otherwise.
      */
    public boolean hasPassengerCount() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'passenger_count' field.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder clearPassengerCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'trip_distance' field.
      * @return The value.
      */
    public double getTripDistance() {
      return trip_distance;
    }


    /**
      * Sets the value of the 'trip_distance' field.
      * @param value The value of 'trip_distance'.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder setTripDistance(double value) {
      validate(fields()[2], value);
      this.trip_distance = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'trip_distance' field has been set.
      * @return True if the 'trip_distance' field has been set, false otherwise.
      */
    public boolean hasTripDistance() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'trip_distance' field.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder clearTripDistance() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'pu_location_id' field.
      * @return The value.
      */
    public java.lang.Long getPuLocationId() {
      return pu_location_id;
    }


    /**
      * Sets the value of the 'pu_location_id' field.
      * @param value The value of 'pu_location_id'.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder setPuLocationId(java.lang.Long value) {
      validate(fields()[3], value);
      this.pu_location_id = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'pu_location_id' field has been set.
      * @return True if the 'pu_location_id' field has been set, false otherwise.
      */
    public boolean hasPuLocationId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'pu_location_id' field.
      * @return This builder.
      */
    public schemaregistry.RideRecordCompatible.Builder clearPuLocationId() {
      pu_location_id = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RideRecordCompatible build() {
      try {
        RideRecordCompatible record = new RideRecordCompatible();
        record.vendorId = fieldSetFlags()[0] ? this.vendorId : (java.lang.String) defaultValue(fields()[0]);
        record.passenger_count = fieldSetFlags()[1] ? this.passenger_count : (java.lang.Integer) defaultValue(fields()[1]);
        record.trip_distance = fieldSetFlags()[2] ? this.trip_distance : (java.lang.Double) defaultValue(fields()[2]);
        record.pu_location_id = fieldSetFlags()[3] ? this.pu_location_id : (java.lang.Long) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<RideRecordCompatible>
    WRITER$ = (org.apache.avro.io.DatumWriter<RideRecordCompatible>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<RideRecordCompatible>
    READER$ = (org.apache.avro.io.DatumReader<RideRecordCompatible>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.vendorId);

    out.writeInt(this.passenger_count);

    out.writeDouble(this.trip_distance);

    if (this.pu_location_id == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeLong(this.pu_location_id);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.vendorId = in.readString();

      this.passenger_count = in.readInt();

      this.trip_distance = in.readDouble();

      if (in.readIndex() != 1) {
        in.readNull();
        this.pu_location_id = null;
      } else {
        this.pu_location_id = in.readLong();
      }

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.vendorId = in.readString();
          break;

        case 1:
          this.passenger_count = in.readInt();
          break;

        case 2:
          this.trip_distance = in.readDouble();
          break;

        case 3:
          if (in.readIndex() != 1) {
            in.readNull();
            this.pu_location_id = null;
          } else {
            this.pu_location_id = in.readLong();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}