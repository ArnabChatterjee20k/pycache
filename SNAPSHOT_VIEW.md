# core
write_value => basically writes value_length + value bytes
write_length(count) -> variable length encoder
write_value(data) ->
    if int:
        bytes write -> 
            int8 marker + 1byte int -> 2 bytes
            int16 marker + 2byte int -> 3 bytes
            int32 marker + 4bytes int -> 5 bytes

    compress data
    if compress_data_length < original_length:
        bytes write compressed marker
        write_length(compress_data_length)
        write bytes(compress data)
    
    write_length(original_length)
    bytes write (data)
________________________________________________________

# Actual snapshot file view for a single key-value
write_length(key_length)
bytes write (value datatype marker)
write_value(key)

value data type -> list/sequences
    write_length(len(value))
    for entry in value:
        bytes write entry datatype
        write_value(entry)

else:
    write_value(entry for entry in value)

# CRC checksum for verification and integrity
writing -> during every write to buffer -> update checksum with the current written data
reading -. after every read update checksum -> update checksum with the current read data