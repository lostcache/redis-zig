const std = @import("std");
const net = std.net;

fn into_address(ip: []const u8, port: u16) !net.Address {
    return try net.Address.resolveIp(ip, port);
}

fn get_server(addr: std.net.Address) !std.net.Server {
    return try addr.listen(.{
        .reuse_address = true,
    });
}

fn make_connection(server: *std.net.Server) !std.net.Server.Connection {
    return try server.accept();
}

fn handle_client(conn: std.net.Server.Connection) !void {
    try conn.stream.writeAll("+PONG\r\n");
}

pub fn main() !void {
    const address = try into_address("127.0.0.1", 6379);

    var server = try get_server(address);
    defer server.deinit();

    while (true) {
        const connection = try make_connection(&server);
        defer connection.stream.close();

        try handle_client(connection);
    }
}
