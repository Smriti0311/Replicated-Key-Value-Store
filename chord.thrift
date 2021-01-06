typedef string UserID

exception SystemException {
  1: optional string message
}
struct message{
  1: string message; 
}

struct server {
  1: string ip;
  2: i32 port;
}

service keyStore {

  bool ping(),
  void set_cordinator(),
  void reset_cordinator(),
  void setServerTable(1: list<server> server_table, 2: i32 curr_node),
  string write_key(1: i32 key, 2: string value),
  void forceful_write_key(1: i32 key, 2: string value),
  void set_consistency(1: string consistency_level),
  string read_key(1: i32 key),
  string return_value(1: i32 key),
  void log(1: string time, 2: string date, 3: string key, 4: string value),
  
}