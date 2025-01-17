import Array "mo:base/Array";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Buffer "mo:base/Buffer";
import Star "mo:star/star";
import VectorLib "mo:vector";
import BTreeLib "mo:stableheapbtreemap/BTree";
import SetLib "mo:map/Set";
import MapLib "mo:map/Map";
import TT "../../../../timerTool/src";
import ICRC72Subscriber "../../../../icrc72-subscriber.mo/src";
import ICRC72Publisher "../../../../icrc72-publisher.mo/src";
// please do not import any types from your project outside migrations folder here
// it can lead to bugs when you change those types later, because migration types should not be changed
// you should also avoid importing these types anywhere in your project directly from here
// use MigrationTypes.Current property instead


module {

  public let BTree = BTreeLib;
  public let Set = SetLib;
  public let Map = MapLib;
  public let Vector = VectorLib;

  public type Namespace = Text;

  public type ICRC16Property = {
        name : Text;
        value : ICRC16;
        immutable : Bool;
    };

    public type ICRC16 = {
        #Array : [ICRC16];
        #Blob : Blob;
        #Bool : Bool;
        #Bytes : [Nat8];
        #Class : [ICRC16Property];
        #Float : Float;
        #Floats : [Float];
        #Int : Int;
        #Int16 : Int16;
        #Int32 : Int32;
        #Int64 : Int64;
        #Int8 : Int8;
        #Map : [(Text, ICRC16)];
        #ValueMap : [(ICRC16, ICRC16)];
        #Nat : Nat;
        #Nat16 : Nat16;
        #Nat32 : Nat32;
        #Nat64 : Nat64;
        #Nat8 : Nat8;
        #Nats : [Nat];
        #Option : ?ICRC16;
        #Principal : Principal;
        #Set : [ICRC16];
        #Text : Text;
    };

    //ICRC3 Value
    public type Value = {
        #Nat : Nat;
        #Int : Int;
        #Text : Text;
        #Blob : Blob;
        #Array : [Value];
        #Map : [(Text, Value)];
    };

    public type ICRC16Map = [(Text, ICRC16)];


      public type GenericError = {
        error_code : Nat;
        message : Text;
    };

    public func mapValueToICRC16(data : Value) : ICRC16 {
        switch (data) {
            case (#Nat(v)) #Nat(v);
            case (#Nat8(v)) #Nat8(v);
            case (#Int(v)) #Int(v);
            case (#Text(v)) #Text(v);
            case (#Blob(v)) #Blob(v);
            case (#Bool(v)) #Bool(v);
            case (#Array(v)) {
                let result = Vector.new<ICRC16>();
                for (item in v.vals()) {
                    Vector.add(result, mapValueToICRC16(item));
                };
                #Array(Vector.toArray(result));
            };
            case (#Map(v)) {
                let result = Vector.new<(Text, ICRC16)>();
                for (item in v.vals()) {
                    Vector.add(result,(item.0, mapValueToICRC16(item.1)));
                };
                #Map(Vector.toArray(result));
            };
        };
    };

    public type Response = {
        #Ok : Value;
        #Err : Text;
    };

    public type EventRelay = {
        id : Nat;
        prevId : ?Nat;
        timestamp : Nat;
        namespace : Text;
        source : Principal;
        data : ICRC16;
        headers : ?ICRC16Map;
    };


    public type EventNotification = {
        id : Nat;
        eventId : Nat;
        prevEventId : ?Nat;
        timestamp : Nat;
        namespace : Text;
        data : ICRC16;
        source : Principal;
        headers : ?ICRC16Map;
        filter : ?Text;
    };

    public type EventNotificationRecord = {
      id : Nat;
      eventId : Nat;
      publication : Text;
      destination: Principal;
      headers : ?ICRC16Map;
      filter : ?Text;
      var bSent : ?Nat;
      var bConfirmed : ?Nat;
      var stake : Nat;
      var timerId : ?Nat;
    };

    public type EventNotificationRecordShared = {
      id : Nat;
      eventId : Nat;
      publication : Text;
      destination: Principal;
      headers : ?ICRC16Map;
      filter : ?Text;
      bSent : ?Nat;
      bConfirmed : ?Nat;
      stake : Nat;
      timerId : ?Nat;
    };

    public func eventNotificationRecordToShared(eventNotificationRecord: EventNotificationRecord) : EventNotificationRecordShared {
      return {
        id = eventNotificationRecord.id;
        eventId = eventNotificationRecord.eventId;
        publication = eventNotificationRecord.publication;
        destination = eventNotificationRecord.destination;
        headers = eventNotificationRecord.headers;
        filter = eventNotificationRecord.filter;
        bSent = eventNotificationRecord.bSent;
        bConfirmed = eventNotificationRecord.bConfirmed;
        stake = eventNotificationRecord.stake;
        timerId = eventNotificationRecord.timerId;
      };
    };

    public type Event = {
      id : Nat;
      prevId : ?Nat;
      timestamp : Nat;
      namespace : Text;
      source : Principal;
      data : ICRC16;
      headers : ?ICRC16Map;
    };

    public type EventRecord = {
      event: Event;
      notificationQueue: Set.Set<Nat>; //managment queue
      relayQueue: Set.Set<Principal>; //managment queue
      var notifications: [Nat]; //permenant list
    };

    public type EventRecordShared = {
      event: Event;
      notificationQueue: [Nat];
      relayQueue: [Principal];
      notifications: [Nat];
    };

    public func eventRecordToShared(eventRecord: EventRecord) : EventRecordShared {
      return {
        event = eventRecord.event;
        notificationQueue = Set.toArray(eventRecord.notificationQueue);
        relayQueue = Set.toArray(eventRecord.relayQueue);
        notifications = eventRecord.notifications;
      };
    };


    public type SubscriberActor = actor {
        icrc72_handle_notification([EventNotification]) : async ();
        icrc72_handle_notification_trusted([EventNotification]) : async {
            #Ok : Value;
            #Err : Text;
        };
    };

    public type PermissionSet = {
        #allowed : Set.Set<Principal>;
        #disallowed : Set.Set<Principal>;
        #allowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
        #disallowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
    };

    public type SubscriberRecord = {
      subscriptionId: Nat;
      publicationId: Nat;
      initialConfig: ICRC16Map;
      subscriber: Principal;
      var filter: ?Text;
      var skip: ?(Nat, Nat);
      namespace: Text;
    };

    public type SubscriberRecordShared = {
      subscriptionId: Nat;
      publicationId: Nat;
      initialConfig: ICRC16Map;
      subscriber: Principal;
      filter: ?Text;
      skip: ?(Nat, Nat);
      namespace: Text;
    };

    public func subscriberRecordToShared(subscriberRecord: SubscriberRecord) : SubscriberRecordShared {
      return {
        subscriptionId = subscriberRecord.subscriptionId;
        publicationId = subscriberRecord.publicationId;
        initialConfig = subscriberRecord.initialConfig;
        subscriber = subscriberRecord.subscriber;
        filter = subscriberRecord.filter;
        skip = subscriberRecord.skip;
        namespace = subscriberRecord.namespace;
      };
    };

    public type PublicationRecord = {
        id : Nat; // Unique identifier for the publication
        namespace : Text; // The namespace of the publication
        registeredPublishers : Set.Set<Principal>; // Map of publishers registered and their 
        registeredSubscribers : BTree.BTree<Principal, SubscriberRecord>; // Map of publishers registered and their assigned broadcasters
        registeredRelay : BTree.BTree<Principal, ?Set.Set<Text>>; // Map of relays registered  and filters
        stakeIndex : BTree.BTree<Nat, BTree.BTree<Nat,Principal>>; //amount, subscriber
        subnetIndex: Map.Map<Principal, Principal>; //subnet, broadcaster
    };

    public type PublicationRecordShared = {
        id : Nat; // Unique identifier for the publication
        namespace : Text; // The namespace of the publication
        registeredPublishers : [Principal]; // Map of publishers registered and their 
        registeredSubscribers : [(Principal, SubscriberRecordShared)]; // Map of publishers registered and their assigned broadcasters
        registeredRelay : [(Principal, ?[Text])]; // Map of relays registered  and filters
        stakeIndex : [(Nat, [(Nat, Principal)])]; //amount, subscriber
        subnetIndex: [(Principal, Principal)]; //subnet, broadcaster
    };

    public func publicationRecordToShared(publicationRecord: PublicationRecord) : PublicationRecordShared {
      return {
        id = publicationRecord.id;
        namespace = publicationRecord.namespace;
        registeredPublishers = Set.toArray(publicationRecord.registeredPublishers);
        registeredSubscribers = Array.map<(Principal, SubscriberRecord), (Principal, SubscriberRecordShared)>(BTree.toArray(publicationRecord.registeredSubscribers), func(entry){(entry.0, subscriberRecordToShared(entry.1))});
        registeredRelay = Array.map<(Principal, ?Set.Set<Text>),(Principal,?[Text])>( BTree.toArray(publicationRecord.registeredRelay),func(entry){
          switch(entry.1){
            case(?filter) (entry.0, ?(Set.toArray(filter)));
            case(null) (entry.0, ?[]);
          };
        });
        stakeIndex = Array.map<(Nat, BTree.BTree<Nat,Principal>), (Nat, [(Nat, Principal)])>(BTree.toArray(publicationRecord.stakeIndex), func(entry){
          let stakeIndex = BTree.toArray(entry.1);
          return (entry.0, stakeIndex);
        });
        subnetIndex = Map.toArray(publicationRecord.subnetIndex);
      };
    };

    public type SubscriptionRecord = {
      id: Nat;
      namespace: Text;
      var filter: ?Text;
      var stake: Nat;
      var skip: ?(Nat, Nat);
      config: ICRC16Map;
    };

    public type SubscriptionRecordShared = {
      id: Nat;
      namespace: Text;
      filter: ?Text;
      stake: Nat;
      skip: ?(Nat, Nat);
      config: ICRC16Map;
    };

    public func subscriptionRecordToShared(subscriptionRecord: SubscriptionRecord) : SubscriptionRecordShared {
      return {
        id = subscriptionRecord.id;
        namespace = subscriptionRecord.namespace;
        filter = subscriptionRecord.filter;
        stake = subscriptionRecord.stake;
        skip = subscriptionRecord.skip;
        config = subscriptionRecord.config;
      };
    };

  ///MARK: Constants
  public let CONST = {
    broadcasters = {
      sys = "icrc72:broadcaster:sys:";
      timer = {
        sendMessage = "icrc72:broadcaster:timer:sendMessage";
        drainMessage = "icrc72:broadcaster:timer:drainMessage"; 
        drainRelay = "icrc72:broadcaster:timer:drainRelay";
      };
      publisher = {
        add = "icrc72:broadcaster:publisher:add";
        remove = "icrc72:broadcaster:publisher:remove";
      };
      subscriber = {
        add = "icrc72:broadcaster:subscriber:add";
        remove = "icrc72:broadcaster:subscriber:remove";
      };
      relay = {
        add = "icrc72:broadcaster:relay:add";
        remove = "icrc72:broadcaster:relay:remove";
      };
      relayer = {
        add = "icrc72:broadcaster:relayer:add";
        remove = "icrc72:broadcaster:relayer:remove";
      };
    };
    publisher = {
      sys = "icrc72:publisher:sys:";
      broadcasters = {
        add = "icrc72:broadcaster:publisher:broadcaster:add";
        remove = "icrc72:broadcaster:publisher:broadcaster:remove";
      };
    };
    subscriber = {
      sys = "icrc72:subscriber:sys:";
      broadcasters = {
        add = "icrc72:broadcaster:subscriber:broadcaster:add";
        remove = "icrc72:broadcaster:subscriber:broadcaster:remove";
      };
    };
  };

  public type InitArgs ={
    name: Text;
  };

  public type Stats ={
    tt: TT.Stats;
    icrc72Subscriber: ICRC72Subscriber.Stats;
    icrc72Publisher: ICRC72Publisher.Stats;
    roundDelay: ?Nat;
    maxMessages: ?Nat;
    icrc72OrchestratorCanister: Principal;
    publications: [(Nat, PublicationRecordShared)];
    subscriptions: [(Nat, SubscriptionRecordShared)];
    eventStore: [(Text, [(Nat, EventRecordShared)])];
    notificationStore: [(Nat, EventNotificationRecordShared)];
    messageAccumulator: [(Principal, [(Nat,EventNotification)])];
    relayAccumulator: [(Principal, [(Nat, Event)])];
    relayTimer: ?Nat;
    messageTimer: ?Nat;
    error: ?Text;
    nextNotificationId: Nat;
  };

  public type Environment = {
    add_record: ?(([(Text, Value)], ?[(Text,Value)]) -> Nat);
    tt: TT.TimerTool;
    icrc72Subscriber : ICRC72Subscriber.Subscriber;
    icrc72Publisher : ICRC72Publisher.Publisher;
    publicationSearch : ?((State, Environment, Text) -> ?Nat);
    subscriptionSearch : ?((State, Environment, Principal, Text) -> ?Nat);
    subscriptionFilter : ?((State, Environment, Text, EventRecord) -> Bool);
    publishReturnFunction : ?((State, Environment, EventRecord) -> [Nat]);
    handleConfirmation : ?(<system>(State, Environment, EventNotificationRecord, EventRecord) -> ());
    handleEventFinalized : ?(<system>(State, Environment, EventRecord) -> ());
    handleBroadcasterListening : ?(<system>(State, Environment, Namespace, Principal, Bool) -> ()); //State, Environment, Namespace, Principal, Listening = True; Resigning = False
    handleBroadcasterPublishing : ?(<system>(State, Environment, Namespace, Principal, Bool) -> ()); //State, Environment, Namespace, Principal, Listening = True; Resigning = False
    roundDelay : ?Nat;
    maxMessages : ?Nat;
    icrc72OrchestratorCanister: Principal;
  };

  ///MARK: State
  public type State = {
    publications : BTree.BTree<Nat, PublicationRecord>;
    publicationsByNamespace : BTree.BTree<Text, Nat>;
    subscriptions : BTree.BTree<Nat, SubscriptionRecord>;
    subscriptionsByNamespace : BTree.BTree<Text, Map.Map<Principal, Nat>>;
    eventStore : BTree.BTree<Text, BTree.BTree<Nat, EventRecord>>; //namespace, event Id
    notificationStore: BTree.BTree<Nat, EventNotificationRecord>;
    messageAccumulator: BTree.BTree<Principal, BTree.BTree<Nat,EventNotification>>;
    relayAccumulator: BTree.BTree<Principal, BTree.BTree<Nat,Event>>;
    var relayTimer : ?Nat;
    var messageTimer : ?Nat;
    var error: ?Text;
    var nextNotificationId: Nat;
  };
};