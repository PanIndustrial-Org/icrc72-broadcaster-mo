import MigrationTypes "/migrations/types";
import {getShared; path } "mo:candy-utils/CandyUtils";

module{

  type State = MigrationTypes.Current.State;
  type Environment = MigrationTypes.Current.Environment;
  type EventRecord = MigrationTypes.Current.EventRecord;

  public func defaultFilter(state: State, environment: Environment, filter: Text, eventRecord : EventRecord): Bool {

    let result = getShared(eventRecord.event.data, path(filter));

    switch(result){
      case(#Option(x)){

        switch(x){
          case(null){
            return false;
          };
          case(_){
            return true;
          };
        }
      };
      case(_){
        return true;
      };
    };
  };
};