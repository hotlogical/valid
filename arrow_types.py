import pyarrow as pa

# A map for arrow types for use in parsing yaml dataset description

types = 'null bool_ int8 int16 int32 int64 uint8 uint16 uint32 uint64 float16 float32 float64 date32 date64 binary string large_binary large_string'.split()

arrow_types = {t: eval(f'pa.{t}()') for t in types}

arrow_types.update({'time32s': pa.time32('s')})
arrow_types.update({'time32ms': pa.time32('ms')})
arrow_types.update({'time64us': pa.time64('us')})
arrow_types.update({'time64ns': pa.time64('ns')})
arrow_types.update({'timestamps': pa.timestamp('s')})
arrow_types.update({'timestampms': pa.timestamp('ms')})
arrow_types.update({'timestampus': pa.timestamp('us')})
arrow_types.update({'timestampns': pa.timestamp('ns')})
arrow_types.update({'durations': pa.timestamp('s')})
arrow_types.update({'durationms': pa.timestamp('ms')})
arrow_types.update({'durationus': pa.timestamp('us')})
arrow_types.update({'durationns': pa.timestamp('ns')})

if __name__ == '__main__':
    print(arrow_types)
