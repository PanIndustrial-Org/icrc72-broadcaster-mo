import MigrationTypes "migrations/types";
import MigrationLib "migrations";
import Service "service";
import BTree "mo:stableheapbtreemap/BTree";
import Map "mo:map/Map";
import Array "mo:base/Array";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Star "mo:star/star";
import Text "mo:base/Text";
import TT "../../timerTool/src/";
import ICRC75Service = "../../ICRC75/src/service";
import ICRC72SubscriberService = "../../icrc72-subscriber.mo/src/service";
import ICRC72OrchestratorService = "../../icrc72-orchestrator.mo/src/service";
import D "mo:base/Debug";
import Error "mo:base/Error";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Buffer "mo:base/Buffer";
import Conversion = "mo:candy/conversion";
import Candy = "mo:candy/types";
import Utils = "utils";
import Result = "mo:base/Result";
import ClassPlusLib "../../../../ICDevs/projects/ClassPlus/src/";

module {

  public let Migration = MigrationLib;

  public type State = MigrationTypes.State;

  public type CurrentState = MigrationTypes.Current.State;

  public type  PublishResult = Service.PublishResult;
  public type ConfirmMessageResult = Service.ConfirmMessageResult;

  public type Environment = MigrationTypes.Current.Environment;
  public type Value = MigrationTypes.Current.Value;
  public type ICRC16 = MigrationTypes.Current.ICRC16;
  public type ICRC16Map = MigrationTypes.Current.ICRC16Map;
  public type Namespace = MigrationTypes.Current.Namespace;
  public type Event = MigrationTypes.Current.Event;
  public type EventRecord = MigrationTypes.Current.EventRecord;
  public type EventRecordShared = MigrationTypes.Current.EventRecordShared;
  public let eventRecordToShared = MigrationTypes.Current.eventRecordToShared;
  public type EventNotificationRecord = MigrationTypes.Current.EventNotificationRecord;
  public type EventNotificationRecordShared = MigrationTypes.Current.EventNotificationRecordShared;
  public let eventNotificationRecordToShared = MigrationTypes.Current.eventNotificationRecordToShared;
  public type EventNotification = MigrationTypes.Current.EventNotification;
  public type PublicationRecord = MigrationTypes.Current.PublicationRecord;
  public type PublicationRecordShared = MigrationTypes.Current.PublicationRecordShared;
  public let publicationRecordToShared = MigrationTypes.Current.publicationRecordToShared;
  public type SubscriberRecord = MigrationTypes.Current.SubscriberRecord;
  public type SubscriptionRecord = MigrationTypes.Current.SubscriptionRecord;
  public type SubscriptionRecordShared = MigrationTypes.Current.SubscriptionRecordShared;
  public let subscriptionRecordToShared = MigrationTypes.Current.subscriptionRecordToShared;
  public type Stats = MigrationTypes.Current.Stats;
  public type InitArgs = MigrationTypes.Current.InitArgs;

  public type PublicationInfo = ICRC72OrchestratorService.PublicationInfo;
  public type SubscriberInfo = ICRC72OrchestratorService.SubscriberInfo;
  public type SubscriptionInfo = ICRC72OrchestratorService.SubscriptionInfo;
  
  public type GenericError = MigrationTypes.Current.GenericError;


  
  public let init = Migration.migrate;
  public let CONST = MigrationTypes.Current.CONST;

  public let Set = MigrationTypes.Current.Set;

  public let Map = MigrationTypes.Current.Map;
  public let {phash; thash; nhash} = Map;
  public let Vector = MigrationTypes.Current.Vector;
  public let BTree = MigrationTypes.Current.BTree;
  //public let governance = MigrationTypes.Current.governance;


  public let ONE_MINUTE = 60000000000 : Nat; //NanoSeconds
  public let FIVE_MINUTES = 300000000000 : Nat; //NanoSeconds
  public let ONE_SECOND = 1000000000 : Nat; //NanoSeconds
  public let THREE_SECONDS = 3000000000 : Nat; //NanoSeconds

  public func initialState() : State {#v0_0_0(#data)};
  public let currentStateVersion = #v0_1_0(#id);

  type ConfigMap  = Map.Map<Text, ICRC16>;

  public type ClassPlus = ClassPlusLib.ClassPlus<
    Broadcaster, 
    State,
    InitArgs,
    Environment>;

  public func ClassPlusGetter(item: ?ClassPlus) : () -> Broadcaster {
    ClassPlusLib.ClassPlusGetter<Broadcaster, State, InitArgs, Environment>(item);
  };

  public func Init<system>(config : {
      manager: ClassPlusLib.ClassPlusInitializationManager;
      initialState: State;
      args : ?InitArgs;
      pullEnvironment : ?(() -> Environment);
      onInitialize: ?(Broadcaster -> async*());
      onStorageChange : ((State) ->())
    }) :()-> Broadcaster{

      D.print("Broadcaster Init");
      switch(config.pullEnvironment){
        case(?val) {
          D.print("pull environment has value");
         
        };
        case(null) {
          D.print("pull environment is null");
        };
      };  
      ClassPlusLib.ClassPlus<system,
        Broadcaster, 
        State,
        InitArgs,
        Environment>({config with constructor = Broadcaster}).get;
    };


  public class Broadcaster(stored: ?State, caller: Principal, canister: Principal, args: ?InitArgs, environment_passed: ?Environment, storageChanged: (State) -> ()){

    public let debug_channel = {
      var publish = true;
      var message = true;
      var confirm = true;
      var setup = true;
    };

    public let environment = switch(environment_passed){
      case(?val) val;
      case(null) {
        D.trap("Environment is required");
      };
    };

    public var maxMessages = switch(environment.maxMessages){
      case(null) 100;
      case(?val) val;
    };



    public var orchestrator : ICRC72OrchestratorService.Service = actor(Principal.toText(environment.icrc72OrchestratorCanister));

    var state : CurrentState = switch(stored){
        case(null) {
          let #v0_1_0(#data(foundState)) = init(initialState(),currentStateVersion, null, canister);
          foundState;
        };
        case(?val) {
          let #v0_1_0(#data(foundState)) = init(val, currentStateVersion, null, canister);
          foundState;
        };
      };

    storageChanged(#v0_1_0(#data(state)));

    private func natNow(): Nat{Int.abs(Time.now())};

    

    private func starError(awaited: Bool, number: Nat, message: Text) : Star.Star<TT.ActionId, TT.Error> {
      if(awaited){
        return #err(#awaited({error_code = number; message = message}));
      } else {
        return #err(#trappable({error_code = number; message = message}));
      }
    };


    //Mark: Configs

    

    //MARK: Queries

    // Returns the publishers known to the Orchestrator canister

    

    ///MARK: Listeners

    type Listener<T> = (Text, T);

   
    /// Generic function to register a listener.
      ///
      /// Parameters:
      ///     namespace: Text - The namespace identifying the listener.
      ///     remote_func: T - A callback function to be invoked.
      ///     listeners: Vec<Listener<T>> - The list of listeners.
      public func registerListener<T>(namespace: Text, remote_func: T, listeners: Vector.Vector<Listener<T>>) {

        debug if(debug_channel.setup) D.print("               BROADCASTER: Register Listener " # debug_show((namespace)));

        let listener: Listener<T> = (namespace, remote_func);
        switch(Vector.indexOf<Listener<T>>(listener, listeners, func(a: Listener<T>, b: Listener<T>) : Bool {
          Text.equal(a.0, b.0);
        })){
          case(?index){
            Vector.put<Listener<T>>(listeners, index, listener);
          };
          case(null){
            Vector.add(listeners, listener);
          };
        };
      };


    public func icrc72_confirm_notifications<system>(caller : Principal, items : [Nat]) : async* ConfirmMessageResult {

      debug if(debug_channel.confirm) D.print("               BROADCASTER: Confirm Messages " # debug_show(items));

      //todo:validate the caller is a subscriber 

      label proc for(thisNotification in items.vals()){
        //get the notification
        let ?notification = BTree.get(state.notificationStore, Nat.compare, thisNotification) else {
          debug if(debug_channel.confirm) D.print("               BROADCASTER: cant find notificaiton " # debug_show(thisNotification));

          continue proc;
        };

        let ?eventStore = BTree.get(state.eventStore, Text.compare, notification.publication) else {
          debug if(debug_channel.confirm) D.print("               BROADCASTER: cant find event " # debug_show(thisNotification));
          continue proc;
        };

        let ?event = BTree.get(eventStore, Nat.compare, notification.eventId)  else {
          debug if(debug_channel.confirm) D.print("               BROADCASTER: cant find event although collection exists " # debug_show(thisNotification));
          continue proc;
        };

        notification.bConfirmed := ?natNow();

        switch(environment.handleConfirmation){
          case(null){
            //todo, move this to a default handler?
            //nothing is going to happen....so do we delete by default
            ignore BTree.delete(state.notificationStore, Nat.compare, thisNotification);
            Set.delete(event.notificationQueue, Set.nhash, thisNotification);

            if(Set.size(event.notificationQueue) == 0){
              switch(environment.handleEventFinalized){
                case(null){
                  
                  ignore BTree.delete(eventStore, Nat.compare, event.event.id);
                  if(BTree.size(eventStore) == 0){
                    ignore BTree.delete(state.eventStore, Text.compare, notification.publication);
                  };
                };
                case(?val){
                  val<system>(state, environment, event);
                };
              };
              
            };

          };
          case(?val){
            //assume thy handle the clean up
            val<system>(state, environment, notification, event);
          };
        };
        
        //todo log the confirmation icrc3.  do we delete it here?


      };

      debug if(debug_channel.confirm) D.print("               BROADCASTER: Confirm Messages Done " # debug_show(items));

      #allAccepted;

    };

    //todo: need a reverse index for cleaning up stake index and updating stake
    private func fileSubscriberInPublication(subscription: SubscriptionRecord, subscriber: SubscriberRecord, publication: PublicationRecord) : () {
      debug if(debug_channel.publish) D.print("               BROADCASTER: File Subscriber in Publication " # debug_show((subscriber, subscription, publication)));

      let foundTree = switch(BTree.get(publication.stakeIndex, Nat.compare, subscription.stake)){
        case(?val) val;
        case(null){
          let newTree = BTree.init<Nat, Principal>(null);
          ignore BTree.insert(publication.stakeIndex, Nat.compare, subscription.stake, newTree);
          newTree;
        };
      };

      ignore BTree.insert(foundTree, Nat.compare, natNow(), subscriber.subscriber);
      ignore BTree.insert(publication.registeredSubscribers, Principal.compare, subscriber.subscriber, subscriber); 
    };

    //environment.tt.registerExecutionListenerAsync(?CONST.publication.actions.canAssignBroadcaster, canAssignBroadcaster );
    public func icrc72_publish<system>(caller: Principal, messages : [Event]) : async* [?PublishResult]{


      debug if(debug_channel.publish) D.print("               BROADCASTER: Publish " # debug_show(messages));

      //todo: validate the sender

      let results = Vector.new<?PublishResult>();

      let roundDelay = (switch(environment.roundDelay){ case(null)(THREE_SECONDS); case(?val)(val)});

      //let results = Array.tabulate(messages.size(), func<system>(i : Nat): ?PublishResult {
      var nonceDelay = 0;

      label proc for(thisItem in messages.vals()){

        //find the publication
        //let thisItem = messages[i];

        debug if(debug_channel.publish) D.print("               BROADCASTER: icrc72_publish " # debug_show(thisItem));


        if(thisItem.namespace == CONST.broadcasters.sys # Principal.toText(canister) and thisItem.source == environment.icrc72OrchestratorCanister){

          debug if(debug_channel.publish) D.print("               BROADCASTER: icrc72_publish system publish detected" # debug_show(thisItem));

          switch(getPublicationByNamespace(thisItem.namespace)){
            case(#ok(val)) {};
            case(#err(err)){

              debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found for sys so discovering " # err # " " # debug_show(thisItem));
              //process the publication
              switch(await* discoverPublication(CONST.broadcasters.sys # Principal.toText(canister))){
                case(#ok(val)) {
                  debug if(debug_channel.message) D.print("               BROADCASTER: discovered " # debug_show(val)); 
                  let publicationRecord = buildPublicationFromInfo(val);

                  filePublication(publicationRecord);
                  Set.add(publicationRecord.registeredPublishers, Set.phash, environment.icrc72OrchestratorCanister);


                  debug if(debug_channel.publish) D.print("               BROADCASTER: filed publication " # debug_show(BTree.toArray(state.publications)));

                  switch(await* discoverSubscriber(CONST.broadcasters.sys # Principal.toText(canister), canister, canister)){
                    case(#ok(thisSubscriber)) {
                      debug if(debug_channel.publish) D.print("               BROADCASTER: discovered subscribers " # debug_show(thisSubscriber));
                      

                      let ?subscriptionSearcher = thisSubscriber.subscriptions else {
                        debug if(debug_channel.publish) D.print("               BROADCASTER: No subscriptions found " # debug_show(thisSubscriber));
                        continue proc;
                      };

                      for(thisSubscription in subscriptionSearcher.vals()){
                        let subscription = switch(BTree.get(state.subscriptions, Nat.compare, thisSubscription)){
                          case(?val) val;
                          case(null){

                            switch(await* discoverSubscription(CONST.broadcasters.sys # Principal.toText(canister))){
                              case(#err(err)){
                                debug if(debug_channel.publish) D.print("               BROADCASTER: No subscriptions found " # err);
                                continue proc;
                              };
                              case(#ok(val)){
                              
                                let thisSubscription = if(val.size() > 0){
                                  val[0];
                                } else {
                                  debug if(debug_channel.publish) D.print("               BROADCASTER: No subscriptions found on discovery " # debug_show(val));
                                  continue proc;
                                };
                            
                                let configMap = Map.fromIter<Text, ICRC16>(thisSubscription.config.vals(), thash);
                                let filter = switch(Map.get(configMap, thash, "filter")){
                                  case(?#Text(val)) ?val;
                                  case(null) null;
                                  case(_) null;
                                };

                                let stake = switch(Map.get(configMap, thash, "stake")){
                                  case(?#Nat(val)) val;
                                  case(null) 0;
                                  case(_) 0;
                                };

                                let skip = switch(Map.get(configMap, thash, "skip")){
                                  case(?#Array(val)){
                                    let skipSeed = switch(val[0]){
                                      case(#Nat(val)) val;
                                      case(_) 0;
                                    };
                                    let offset = switch(val[0]){
                                      case(#Nat(val)) val;
                                      case(_) 0;
                                    };
                                    ?(skipSeed, offset);
                                  };
                                  
                                  case(_) null;
                                };
                            
                                let newSubscription : SubscriptionRecord = {
                                  id = val[0].subscriptionId;
                                  namespace = val[0].namespace;
                                  var filter = filter;
                                  var stake = stake;
                                  var skip = skip;
                                  config = val[0].config;
                                };
                                fileSubscription(thisSubscriber.subscriber, newSubscription);

                                newSubscription;
                              };
                            };
                          };
                        };

                        debug if(debug_channel.publish) D.print("               BROADCASTER: discovered new subscription " # debug_show(subscription));

                        let configMap = Map.fromIter<Text, ICRC16>(thisSubscriber.config.vals(), thash);
                        let filter = switch(Map.get(configMap, thash, "filter")){
                          case(?#Text(val)) ?val;
                          case(null) null;
                          case(_) null;
                        };

                        let skip = switch(Map.get(configMap, thash, "skip")){
                          case(?#Array(val)){
                            let skipSeed = switch(val[0]){
                              case(#Nat(val)) val;
                              case(_) 0;
                            };
                            let offset = switch(val[0]){
                              case(#Nat(val)) val;
                              case(_) 0;
                            };
                            ?(skipSeed, offset);
                          };
                          
                          case(_) null;
                        };

                        let subscriberRecord : SubscriberRecord = {
                          subscriptionId = subscription.id;
                          publicationId = publicationRecord.id;
                          var filter = filter;
                          var skip = skip;
                          subscriber = thisSubscriber.subscriber;
                          initialConfig = subscription.config;
                          namespace = subscription.namespace;
                        };

                        fileSubscriberInPublication(subscription, subscriberRecord, publicationRecord);
                      };
                    };
                    case(#err(err)){
                      debug if(debug_channel.publish) D.print("               BROADCASTER: No subscribers found " # err);
                    };
                  };

                  debug if(debug_channel.publish) D.print("               BROADCASTER: do subs exist " # debug_show(BTree.toArray(state.subscriptions)));
                };
                case(#err(err)){
                  debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found for sys " # err # " " # debug_show(thisItem));
                  continue proc;
                };
              };
            };
          };
        };

        debug if(debug_channel.publish) D.print("               BROADCASTER: icrc72_publish normal publish detected made it past sys " # debug_show(thisItem));

        let publication = switch(getPublicationByNamespace(thisItem.namespace)){
          case(#ok(val)) val;
          case(#err(err)){
            debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found icrc72_publish " # err # " " # debug_show(thisItem));
            Vector.add(results, ?#Err(#PublicationNotFound));
            continue proc;
          };
        };

        debug if(debug_channel.publish) D.print("               BROADCASTER: publication found " # debug_show(publication));

        let originalHeaders = let headerMap = switch(thisItem.headers){
          case(null) Map.new<Text, ICRC16>();
          case(?val) Map.fromIter<Text, ICRC16>(val.vals(), Map.thash);
        };

        let newHeaders = switch(thisItem.headers){
          case(null) Map.new<Text, ICRC16>();
          case(?val) Map.fromIter<Text, ICRC16>(val.vals(), Map.thash);
        };

        debug if(debug_channel.publish) D.print("               BROADCASTER: headers " # debug_show((originalHeaders, newHeaders))); 
        

        //validate the publisher

        
        debug if(debug_channel.publish) D.print("               BROADCASTER: publisher " # debug_show(caller) # " " # debug_show(publication.registeredPublishers)); 
        if(Set.has(publication.registeredPublishers, Set.phash, caller) == false 
        //and false == true
        ) {
          debug if(debug_channel.publish) D.print("               BROADCASTER: publisher " # debug_show(caller) # " not allowed for " # debug_show(thisItem));

          //test for relay
          switch(BTree.get(publication.registeredRelay, Principal.compare, caller)){
            case(null){
              debug if(debug_channel.publish) D.print("               BROADCASTER: publisher " # debug_show(caller) # " not allowed for " # debug_show(thisItem));
              Vector.add(results, ?#Err(#Unauthorized));
              continue proc;
            };
            case(?val){
            //add the header
              switch(Map.get(originalHeaders, Map.thash, "relay")){
                case(?val){
                  debug if(debug_channel.publish) D.print("               BROADCASTER: double relay from " # debug_show(caller) # " relay val " # debug_show(thisItem));
                  Vector.add(results, ?#Err(#Unauthorized));
                  continue proc;
                };
                case(null){
                  debug if(debug_channel.publish) D.print("               BROADCASTER: relay from " # debug_show(caller) # " relay null " # debug_show(thisItem));
                  //add the header
                  ignore Map.add(newHeaders, Map.thash, "broadcaster", #Blob(Principal.toBlob(canister)));
                };
              };
            };
          };
          
          
          //return ?#Err(#Unauthorized);
        } else {
          //make sure there is not already a broadcast header
          switch(Map.get(originalHeaders, Map.thash, "broadcaster")){
            case(?val){
              debug if(debug_channel.publish) D.print("               BROADCASTER: double broadcaster " # debug_show(caller) # " not allowed for " # debug_show(thisItem));
              Vector.add(results, ?#Err(#Unauthorized));
              continue proc;
            };
            case(null){
              //add the header
              ignore Map.add(newHeaders, Map.thash, "broadcaster", #Blob(Principal.toBlob(canister)));
            };
          }
          //add the header
        };

        //validate the 

        

        //store the Event
        let eventRecord : EventRecord = {
          event = thisItem;
          notificationQueue = Set.new<Nat>();
          relayQueue = Set.new<Principal>();
          var notifications : [Nat] = [];
        };
    

        debug if(debug_channel.publish) D.print("               BROADCASTER: eventRecord " # debug_show(eventRecord, thisItem.id, ));

        let eventCol = switch(BTree.get(state.eventStore, Text.compare, thisItem.namespace)){
          case(?val)val;
          case(null){
            let newEventStore = BTree.init<Nat, EventRecord>(null);
            ignore BTree.insert(state.eventStore, Text.compare, thisItem.namespace, newEventStore);
            newEventStore;
          };
        };

        ignore BTree.insert(eventCol, Nat.compare, thisItem.id, eventRecord);
        debug if(debug_channel.publish) D.print("               BROADCASTER: publicationIndex " # debug_show(BTree.toArray(publication.stakeIndex)));
     

        if(BTree.size(publication.stakeIndex) > 0){
          

          //get the proper order
          let ?max = BTree.max(publication.stakeIndex) else {
            debug if(debug_channel.publish) D.print("               BROADCASTER: No subscribers " # debug_show(publication));
            Vector.add(results, ?#Ok([]));
            continue proc;
            //return ?#Ok([]); //todo: change to no subscribers
          };

          //current order is stake then registration time

          let subscribersStakes = BTree.scanLimit(publication.stakeIndex, Nat.compare, max.0, 0, #bwd, BTree.size(publication.stakeIndex));

          let subscribers = Buffer.Buffer<(Nat, Principal)>(1);

          for(thisGroup in subscribersStakes.results.vals()){
            for(thisItem in BTree.entries(thisGroup.1)){
              subscribers.add(thisGroup.0, thisItem.1);
            };
          };

          debug if(debug_channel.publish) D.print("               BROADCASTER: subscribers " # debug_show((Buffer.toArray(subscribers), subscribers.size())));

          //enque the items
          var delay = 0;
          var perRound = 1;
          var numberProcessed = 0;
          let now = natNow();

          label enque for(thisNotification : (Nat, Principal) in subscribers.vals()){

            debug if(debug_channel.publish) D.print("               BROADCASTER: enque " # debug_show(thisNotification));

            let ?subscriptionRecord = BTree.get(publication.registeredSubscribers, Principal.compare, thisNotification.1) else {
              debug if(debug_channel.publish) D.print("               BROADCASTER: No subscriber found " # debug_show(thisNotification));
              continue enque;
            };

            debug if(debug_channel.publish) D.print("               BROADCASTER: found subscription record " # debug_show(subscriptionRecord));

            let filter = switch(subscriptionRecord.filter){
              case(null) null;
              case(?val){
                // run the filter
                let filterResult = switch(environment.subscriptionFilter){
                  case(?filterFunction) filterFunction(state, environment, val, eventRecord);
                  case(null) Utils.defaultFilter(state, environment, val, eventRecord);
                };
                if(filterResult == false){
                  continue enque;
                };
                ?val;
              };
            };

            let nextNotificationId = state.nextNotificationId;
                

            debug if(debug_channel.publish) D.print("               BROADCASTER: enqueue " # debug_show((nextNotificationId, numberProcessed, Principal.toBlob(canister), eventRecord, Buffer.toArray(subscribers), thisNotification)));

              //schedule the execution
            let notificaitionRecord : EventNotificationRecord = {
              id = nextNotificationId;
              eventId = thisItem.id;
              destination = thisNotification.1;
              publication = thisItem.namespace;
              headers = switch(thisItem.headers){
                case(null) ?[("broadcaster" , #Blob(Principal.toBlob(canister)))]; 
                case(?val) ?Array.append<(Text, ICRC16)>(val, [("broadcaster" , #Blob(Principal.toBlob(canister)))]);
              }; //todo: create interface to add headers
              filter = filter;
              var bSent = null;
              var bConfirmed = null;
              var stake = thisNotification.0;

              //schedule the execution
              var timerId = do {
                let result = environment.tt.setActionASync<system>(now + delay, {
                  actionType = CONST.broadcasters.timer.sendMessage;
                  params = to_candid(nextNotificationId);
                }, FIVE_MINUTES);
                nonceDelay := nonceDelay + 1;
                ?result.id;
              };
            };

            debug if(debug_channel.publish) D.print("               BROADCASTER: enque " # debug_show(notificaitionRecord));

            ignore BTree.insert(state.notificationStore, Nat.compare, notificaitionRecord.id, notificaitionRecord); 

            Set.add(eventRecord.notificationQueue, Set.nhash, notificaitionRecord.id);
            
            state.nextNotificationId := state.nextNotificationId + 1;
            numberProcessed := numberProcessed + 1;

            if(numberProcessed == perRound){
              perRound := perRound * 2;
              delay := delay + (roundDelay);
              numberProcessed := 0;
            };

          };
          
          let thisResult = switch(environment.publishReturnFunction){
            case(?returnFunction) returnFunction(state, environment, eventRecord);
            case(null) Set.toArray(eventRecord.notificationQueue);
          };

          Vector.add(results, ?#Ok(thisResult));
        };

        //handle relays
        //todo: extend stake to relays
        debug if(debug_channel.publish) D.print("               BROADCASTER: handling relays " # debug_show(eventRecord));

        label relay for(thisRelay in BTree.entries(publication.registeredRelay)){

          let now = natNow();

          debug if(debug_channel.publish) D.print("               BROADCASTER: handling relay item " # debug_show(thisRelay)); 

          let filter = switch(thisRelay.1){
            case(null) null;
            case(?val){
              var bFound = false;
              label filter for(thisFilter in Set.keys(val)){
                let filterResult = switch(environment.subscriptionFilter){
                  case(?filterFunction) filterFunction(state, environment, thisFilter, eventRecord);
                  case(null) Utils.defaultFilter(state, environment, thisFilter, eventRecord);
                };
                if(filterResult == true){
                  bFound := true; //only need to find one match to need to relay
                  break relay;
                };
              };
              if(bFound == false){
                continue relay;
              };
              
              ?val;
            };
          };
        
          Set.add(eventRecord.relayQueue, Set.phash, thisRelay.0);
          let accumulator = switch(BTree.get(state.relayAccumulator, Principal.compare, thisRelay.0)){
            case(null){
              let newAccumulator = BTree.init<Nat,Event>(null);
              ignore BTree.insert(state.relayAccumulator, Principal.compare, thisRelay.0, newAccumulator);
              newAccumulator;
            };
            case(?val) val;
          };

          
          ignore BTree.insert(accumulator, Nat.compare, thisItem.id, thisItem);
          
          if(state.relayTimer == null){
            let result = environment.tt.setActionSync<system>(now, {
              actionType = CONST.broadcasters.timer.drainRelay;
              params = to_candid([]);
            });
            state.relayTimer := ?result.id;
          };
        };

        

        continue proc;
        //return ?#Ok(thisResult);
      };

      debug if(debug_channel.publish) D.print("               BROADCASTER: Publish Done " # debug_show(results));

      //return the results
      Vector.toArray(results);
    };


    /* //trying something new:
    private func handleTimerMessage<system>(notificationId: Nat) : ?Nat {

      let ?notificationRecord = BTree.get(state.notificationStore, Nat.compare, notificationId) else{
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid notification record " # debug_show(notificationId));
        return null;
      };

      let ?eventRecord = BTree.get(state.eventStore, Nat.compare, notificationRecord.eventId) else{
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid event  " # debug_show(notificationId));
        return null;
      };

      let accumulator = switch(BTree.get(state.messageAccumulator, Principal.compare, notificationRecord.destination)){
        case(null){
          let newAccumulator = Vector.new<EventNotification>();
          ignore BTree.insert(state.messageAccumulator, Principal.compare, notificationRecord.destination, newAccumulator);
          newAccumulator;
        };
        case(?val) val;
      };

      Vector.add(accumulator, {
        id = notificationRecord.id;
        eventId = notificationRecord.eventId;
        prevEventId = eventRecord.event.prevId;
        timestamp = eventRecord.event.timestamp;
        namespace = eventRecord.event.namespace;
        data = eventRecord.event.data;
        source =eventRecord.event.source;
        headers = notificationRecord.headers;
        filter = notificationRecord.filter;
      } : EventNotification);

      //todo: see if the accumulator is full and send the message
      if(state.messageTimer == null){
        let result = environment.tt.setActionSync<system>(natNow(), {
          actionType = CONST.broadcasters.timer.drainMessage;
          params = to_candid([]);
        });
        state.messageTimer := ?result.id;
      };

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Timer Message " # debug_show(BTree.toArray(state.messageAccumulator)));

      return state.messageTimer;
    }; */

    private func handleTimerMessage<system>(id: TT.ActionId, action: TT.Action) : TT.ActionId{

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Timer Message " # debug_show(action));
      //add items to an outgoing message up to x messages
      let ?notificationId : ?Nat = from_candid(action.params) else{
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid notification id " # debug_show(action));
        return id;
      };

      let ?notificationRecord = BTree.get(state.notificationStore, Nat.compare, notificationId) else{
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid notification record " # debug_show(action));
        return id;
      };

      let ?eventRecord = switch(BTree.get(state.eventStore, Text.compare, notificationRecord.publication)){
        case(?val) BTree.get(val, Nat.compare, notificationRecord.eventId);
        case(null) {
          debug if(debug_channel.message) D.print("               BROADCASTER: Invalid event but had collection  " # debug_show(action));
          null;
        };
      } else{
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid event  " # debug_show(action));
        return id;
      };

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Timer Message " # debug_show(notificationId, notificationRecord, eventRecord));

      let accumulator = switch(BTree.get(state.messageAccumulator, Principal.compare, notificationRecord.destination)){
        case(null){
          let newAccumulator = BTree.init<Nat,EventNotification>(null);
          ignore BTree.insert(state.messageAccumulator, Principal.compare, notificationRecord.destination, newAccumulator);
          newAccumulator;
        };
        case(?val) val;
      };

      ////todo: we may need to extend the accumulator to handle not just event ids, but also notificatio ids to enforce order
      ignore BTree.insert(accumulator, Nat.compare, notificationRecord.eventId,{
        id = notificationRecord.id;
        eventId = notificationRecord.eventId;
        prevEventId = eventRecord.event.prevId;
        timestamp = eventRecord.event.timestamp;
        namespace = eventRecord.event.namespace;
        data = eventRecord.event.data;
        source =eventRecord.event.source;
        headers = notificationRecord.headers;
        filter = notificationRecord.filter;
      } : EventNotification);

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Timer Message " # debug_show(BTree.toArray(state.messageAccumulator)));

      if(state.messageTimer == null){
        let result = environment.tt.setActionSync<system>(natNow(), {
          actionType = CONST.broadcasters.timer.drainMessage;
          params = to_candid([]);
        });
        state.messageTimer := ?result.id;
      };

      return id;
    };

    private func handleDrainRelay<system>(id: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error>  {

      //no actions, just trigger the batch
      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Drain Relay " # debug_show(action));

      let relayToProc = BTree.toArray(state.relayAccumulator);
      BTree.clear(state.relayAccumulator);
      state.relayTimer := null;

      var limiter = 0;

      //handle any relays
      for(accumulator in relayToProc.vals()){
        
        debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch accumulator " # debug_show(accumulator));

        let messages = BTree.toValueArray(accumulator.1);
        let publisherActor : Service.Service = actor(Principal.toText(accumulator.0));
        ignore publisherActor.icrc72_publish(messages);
        limiter := limiter + 1;


        label markSent for(thisItem in BTree.toValueArray(accumulator.1).vals()){
          let ?eventRecord = switch(BTree.get(state.eventStore, Text.compare, thisItem.namespace)){
            case(?val) BTree.get(val, Nat.compare, thisItem.id);
            case(null) null;
          } else {
            debug if(debug_channel.message) D.print("               BROADCASTER: No event found " # debug_show(thisItem));
            continue markSent;
          };

          ignore Set.remove(eventRecord.relayQueue, Set.phash, accumulator.0);
        };

        //todo: need a better manager for outgoing queue
        //todo: need to make sure we have enough cycles
        if(limiter > 400){
          debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message accumulated relay" # debug_show(id));
          await localawait();
          limiter := 0;
        };
      };

      return #awaited(id);
    };

    //todo:  I feel like I had some elaborate set up here to keep from having to await, but I can't remember what it was. This is naieve and just proceses everything
    private func handleMessageBatch<system>(id: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch " # debug_show(action));

      var limiter = 0;

      state.messageTimer := null;

      let toProc = BTree.toArray(state.messageAccumulator);
      BTree.clear(state.messageAccumulator);

      for(accumulator in toProc.vals()){

        debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch accumulator " # debug_show(accumulator));
       
        let messages = BTree.toValueArray(accumulator.1);
        debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch messages " # debug_show(messages));
        let subscriberActor : ICRC72SubscriberService.Service = actor(Principal.toText(accumulator.0));


        subscriberActor.icrc72_handle_notification(messages);


        label markSent for(thisItem in BTree.toValueArray(accumulator.1).vals()){
          switch(BTree.get(state.notificationStore, Nat.compare, thisItem.id)) {
            case(?notificationRecord){
              notificationRecord.bSent := ?natNow();
            };
            case(null) {};
          };

          let ?eventRecord = switch(BTree.get(state.eventStore, Text.compare, thisItem.namespace)){
            case(?val) BTree.get(val, Nat.compare, thisItem.eventId);
            case(null) null;
          } else {
            debug if(debug_channel.message) D.print("               BROADCASTER: No event found " # debug_show(thisItem));
            continue markSent;
          };

          ignore Set.remove(eventRecord.notificationQueue, Set.nhash, thisItem.id);
        };

        //todo: need a better manager for outgoing queue
        //todo: need to make sure we have enough cycles
        limiter := limiter + 1;
        if(limiter > 400){
          debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch accumulated messages" # debug_show(id));
          await localawait();
          limiter := 0;
        };
      };

      

      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Message Batch Done " # debug_show(id));

      return #awaited(id);
    };

    //todo: need to force a wait
    private func localawait() : async (){};

    private func filePublication(publication: PublicationRecord) : () {
      debug if(debug_channel.publish) D.print("               BROADCASTER: File Publication in broadcaster" # debug_show(publication));

      //todo: what if it existed?
      ignore BTree.insert(state.publications, Nat.compare, publication.id, publication);
      ignore BTree.insert(state.publicationsByNamespace, Text.compare, publication.namespace, publication.id);
    };

    private func fileSubscription(subscriber: Principal, subscription: SubscriptionRecord) : () {
      debug if(debug_channel.publish) D.print("               BROADCASTER: File Subscription in broadcaster" # debug_show(subscription));
      ignore BTree.insert(state.subscriptions, Nat.compare, subscription.id, subscription);
      let existing = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, subscription.namespace)){
        case(?val) val;
        case(null) {
          let newIndex = Map.new<Principal, Nat>();

          ignore BTree.insert(state.subscriptionsByNamespace, Text.compare, subscription.namespace, newIndex);
          newIndex;
        };
      };

      ignore Map.add(existing, phash, subscriber, subscription.id);
    };

    private func deletePublication(publication: PublicationRecord) : () {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Delete Publication in broadcaster" # debug_show(publication));
      ignore BTree.delete(state.publications, Nat.compare, publication.id);
      ignore BTree.delete(state.publicationsByNamespace, Text.compare, publication.namespace);
    };

    private func deleteSubscription(id: Nat, namespace: Text) : () {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Delete Subscription in broadcaster" # debug_show((id, namespace)));
      ignore BTree.delete(state.subscriptions, Nat.compare, id);
      ignore BTree.delete(state.subscriptionsByNamespace, Text.compare, namespace);
    };

    public func getPublicationByNamespace(namespace: Text) : Result.Result<PublicationRecord, Text> {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Get Publication By Namespace " # debug_show((namespace, BTree.toArray(state.publicationsByNamespace))));

      let ?publicationPointer : ?Nat = switch(environment.publicationSearch){
        case(?searchFunction) searchFunction(state, environment, namespace);
        case(null) BTree.get(state.publicationsByNamespace, Text.compare, namespace) 
      } else {
        debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found getPublicationByNamespace " # debug_show(namespace));
        return #err("No publication found");
      };

      let ?publication = BTree.get(state.publications, Nat.compare, publicationPointer) else {
        debug if(debug_channel.publish) D.print("               BROADCASTER: Pub Pointer " # debug_show(publicationPointer) # " but publication found " # debug_show(namespace));
        return #err("No publication found lookup");
      };

      #ok(publication);
    };


    public func getSubscriptionByNamespace(canister: Principal, namespace: Text) : Result.Result<SubscriptionRecord, Text> {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Get Subscription By Namespace " # debug_show((namespace, BTree.toArray(state.subscriptionsByNamespace))));
      let ?subscriptionPointer : ?Nat = switch(environment.subscriptionSearch){
        case(?searchFunction) searchFunction(state, environment, canister, namespace);
        case(null) {
          switch(BTree.get(state.subscriptionsByNamespace, Text.compare, namespace)){
            case(?val) {
              let subscriptionPointer = Map.get(val, phash, canister);
            };
            case(null) {
              debug if(debug_channel.publish) D.print("               BROADCASTER: No subscription found getSubscriptionByNamespace" # debug_show(namespace));
              return #err("No subscription found");
            };
          }
        };
      } else {
        debug if(debug_channel.publish) D.print("               BROADCASTER: No subscription found getSubscriptionByNamespace" # debug_show(namespace));
        return #err("No subscription found");
      };

      let ?subscription = BTree.get(state.subscriptions, Nat.compare, subscriptionPointer) else {
        debug if(debug_channel.publish) D.print("               BROADCASTER: Sub Pointer " # debug_show(subscriptionPointer) # " but subscription not found " # debug_show(namespace));
        return #err("No subscription found lookup");
      };

      #ok(subscription);
    };

    private func discoverPublication(namespace: Text) : async* Result.Result<PublicationInfo, Text> {

      debug if(debug_channel.publish) D.print("               BROADCASTER: Discover Publication " # debug_show(namespace));

      let publicationResult = try{
        await orchestrator.icrc72_get_publications({
          prev = null;
          take = null; 
          filter = ?{
            statistics = null;
            slice = [#ByNamespace(namespace)];
          }
        });
      } catch(err){
        return #err("Error getting publication " # Error.message(err));
      };

      if(publicationResult.size() == 0){

        debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found discoverPublication" # debug_show(namespace));
        return #err("No publication found "  # " " # namespace);
      } else {
        //todo: retrieve stake?
        
        return #ok(publicationResult[0]);
      };
    };

    private func buildPublicationFromInfo(info : PublicationInfo) : PublicationRecord{
      let publicationRecord : PublicationRecord = {
        id = info.publicationId;
        namespace = info.namespace;
        registeredPublishers = Set.new<Principal>();
        registeredSubscribers = BTree.init<Principal, SubscriberRecord>(null);
        registeredRelay = BTree.init<Principal, ?Set.Set<Text>>(null);
        stakeIndex = BTree.init<Nat, BTree.BTree<Nat,Principal>>(null);
        subnetIndex = Map.new<Principal, Principal>();
      };
      publicationRecord;
    };

    private func discoverSubscriber(namespace: Text, broadcaster: Principal, subscriber: Principal) : async* Result.Result<SubscriberInfo, Text> {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Discover Subscriber " # debug_show((namespace, broadcaster, subscriber)));
      let subscriberResults = try{
        await orchestrator.icrc72_get_subscribers({
          prev = null;
          take = null; 
          filter = ?{
            statistics = null;
            slice = [#ByBroadcaster(broadcaster), #ByNamespace( namespace), #BySubscriber(subscriber)];
          }
        });
      } catch(err){
        return #err("Error getting subscriber " # Error.message(err));
      };

      if(subscriberResults.size() == 0){

        debug if(debug_channel.publish) D.print("               BROADCASTER: No subscribers found subscriber" # debug_show(namespace));
        return #err("No subscriber found "  # " " # namespace);
      } else {
        //todo: retrieve stake?
        
        return #ok(subscriberResults[0]);
      };
    };

    private func discoverSubscription(namespace: Text) : async* Result.Result<[SubscriptionInfo], Text> {
      debug if(debug_channel.publish) D.print("               BROADCASTER: Discover Subscription " # debug_show(namespace));
      let subscriptionResults = try{
        await orchestrator.icrc72_get_subscriptions({
          prev = null;
          take = null; 
          filter = ?{
            statistics = null;
            slice = [#ByNamespace(namespace)];
          }
        });
      } catch(err){
        return #err("Error getting subscription " # Error.message(err));
      };

      if(subscriptionResults.size() == 0){

        debug if(debug_channel.publish) D.print("               BROADCASTER: No subscribers found discoversubscription" # debug_show(subscriptionResults));
        return #err("No publication found "  # " " # namespace);
      } else {
        //todo: retrieve stake?
        
        return #ok(subscriptionResults);
      };
    };


    private func handleOchestraterEvents<system>(notification: EventNotification) : async* (){
      debug if(debug_channel.message) D.print("               BROADCASTER: Handle Orchestrator Event in broadcaster" # debug_show(notification));

      let #Map(data) = notification.data else {
        debug if(debug_channel.message) D.print("               BROADCASTER: Invalid data " # debug_show(notification));
        return;
      };

      label proc for(thisItem in data.vals()) {

        debug if(debug_channel.message) D.print("               BROADCASTER: Handle Orchestrator Event in broadcaster" # debug_show(thisItem));


        if(thisItem.0 == CONST.broadcasters.publisher.add){
          //add the publisher to this broadcaster
          debug if(debug_channel.message) D.print("               BROADCASTER: Publisher add " # debug_show(thisItem));

          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisPublisher in list.vals()){
            let #Array(publisherInfo) = thisPublisher else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(publicationNamespace) = publisherInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(publisherPrincipalBlob) = publisherInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add Blob " # debug_show(thisItem));
              continue proc;
            };
            

            //check that we know about the publication
            debug if(debug_channel.message) D.print("               BROADCASTER: Publisher add checking namespace" # debug_show((publicationNamespace, Principal.fromBlob(publisherPrincipalBlob))));

            let publication = switch(getPublicationByNamespace(publicationNamespace)){
              case(#ok(val)) val;
              case(#err(err)){
                debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add...trying to discover " # debug_show(thisItem));
                let publicationResult = switch(await* discoverPublication(publicationNamespace)){
                  case(#ok(val)) {
                    //recheck that we haven't found it elsewhere
                    debug if(debug_channel.message) D.print("               BROADCASTER: discovered " # debug_show(publicationNamespace, val));

                    let stillMissing = switch(getPublicationByNamespace(publicationNamespace)){
                      case(#ok(val)) val;
                      case(#err(err)) {
                        debug if(debug_channel.message) D.print("               BROADCASTER: discovered " # debug_show(val));
                        let publicationRecord = buildPublicationFromInfo(val);
                        filePublication(publicationRecord);

                        //todo: do we need to get the stake info?
                        publicationRecord;
                      };
                    };
                    stillMissing;
                  };
                  case(#err(err)){
                    debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found handleOchestraterEvents publisher add " # err # " " # debug_show(thisItem));
                    continue proc;
                  };
                };
              };
            };



            let principal = Principal.fromBlob(publisherPrincipalBlob);

            if(Set.has(publication.registeredPublishers, Set.phash,principal) ==true){
             
                //todo: may not be necessary but doing it anyway and rebroadcasting event
                Set.add(publication.registeredPublishers, Set.phash, principal);
                ignore environment.icrc72Publisher.publish<system>([{
                      namespace = CONST.publisher.sys # Principal.toText(principal);
                      data = #Map([(CONST.publisher.broadcasters.add, #Array([#Array([#Text(publication.namespace), #Blob(Principal.toBlob(canister))])]))]);
                      headers = null;
                    }]);
                debug if(debug_channel.message) D.print("               BROADCASTER: Publisher already added " # debug_show(thisItem));
            } else {

                debug if(debug_channel.message) D.print("               BROADCASTER: Publisher add principal to registered " # debug_show(principal));
                Set.add(publication.registeredPublishers, Set.phash, principal);

                //we now need to notify the publisher that we are ready to receive messages
                switch(environment.handleBroadcasterListening){
                  case(null){
                    debug if(debug_channel.message) D.print("               BROADCASTER: No handleBroadcasterListening " # debug_show(thisItem));
                    debug if(debug_channel.message) D.print("               BROADCASTER: Sending event to publisher about broadcaster ready " # debug_show((CONST.publisher.sys # Principal.toText(principal), publication.namespace, Principal.toBlob(canister) )));
                    ignore environment.icrc72Publisher.publish<system>([{
                      namespace = CONST.publisher.sys # Principal.toText(principal);
                      data = #Map([(CONST.publisher.broadcasters.add, #Array([#Array([#Text(publication.namespace), #Blob(Principal.toBlob(canister))])]))]);
                      headers = null;
                    }])

                  };
                  case(?val) val<system>(state, environment, publication.namespace, Principal.fromBlob(publisherPrincipalBlob), true);
                };
              
            };
          };
          
        } else if(thisItem.0 == CONST.broadcasters.publisher.remove){
          debug if(debug_channel.message) D.print("               BROADCASTER: Publisher remove " # debug_show(thisItem));
          //remove the publisher from this broadcaster
          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisPublisher in list.vals()){
            let #Array(publisherInfo) = thisPublisher else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher remove info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(publicationNamespace) = publisherInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher remove namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(publisherPrincipalBlob) = publisherInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher remove Blob " # debug_show(thisItem));
              continue proc;
            };
            

            //check that we know about the publication

            let publication = switch(getPublicationByNamespace(publicationNamespace)){
              case(#ok(val)) val;
              case(#err(err)){
                debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher remove  " # debug_show(thisItem));
              continue proc;
                
              };
            };

            let principal = Principal.fromBlob(publisherPrincipalBlob);

            Set.delete(publication.registeredPublishers, Set.phash,principal);
            if(Set.size(publication.registeredPublishers) == 0 and BTree.size(publication.registeredSubscribers) == 0 and BTree.size(publication.registeredRelay) == 0){
              deletePublication(publication);
            };

            //we now need to notify the publisher that we are no longer receiving messages
            switch(environment.handleBroadcasterListening){
              case(null){
                ignore environment.icrc72Publisher.publish<system>([{
                  namespace = CONST.publisher.sys # Principal.toText(principal);
                  data = #Map([(CONST.publisher.broadcasters.remove, #Array([#Blob(Principal.toBlob(canister))]))]);
                  headers = null;
                }])

              };
              case(?val) val<system>(state, environment, publication.namespace, Principal.fromBlob(publisherPrincipalBlob), false);
            };

            //todo: do we need to let relays and subscirbers know here?
          };
          
        } else if(thisItem.0 == CONST.broadcasters.subscriber.add){
          debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber add " # debug_show(thisItem));
          //add the subscriber to this broadcaster
          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisSubscriber in list.vals()){
            debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber add " # debug_show(thisSubscriber, thisItem));

            let #Array(subsciberInfo) = thisSubscriber else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(subscriptionNamespace) = subsciberInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(subscriberPrincipalBlob) = subsciberInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add Blob " # debug_show(thisItem));
              continue proc;
            };
            
            let subscriberPrincipal = Principal.fromBlob(subscriberPrincipalBlob);
            //check that we know about the publication
            debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber add " # debug_show((subscriptionNamespace, subscriberPrincipal, thisItem)));


            let publication = switch(getPublicationByNamespace(subscriptionNamespace)){
              case(#ok(val)) val;
              case(#err(err)){
                debug if(debug_channel.publish) D.print("               BROADCASTER: Invalid publication add...trying to discover " # debug_show(thisItem));
                let publicationResult = switch(await* discoverPublication(subscriptionNamespace)){
                  case(#ok(val)) {
                    let stillMissing = switch(getPublicationByNamespace(subscriptionNamespace)){
                      case(#ok(val)) val;
                      case(#err(err)) {
                        debug if(debug_channel.publish) D.print("               BROADCASTER: discovered " # debug_show(val));
                        let publicationRecord = buildPublicationFromInfo(val);

                        filePublication(publicationRecord);
                        publicationRecord;
                      };
                      
                    };
                    stillMissing;
                  };
                  case(#err(err)){
                    debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found subscriber add " # err # " " # debug_show(thisItem));
                    continue proc;
                  };
                };

                //todo:retrieve valid publishers
              };
            };

            debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber add publication " # debug_show(publication));

            let subscription : SubscriptionRecord = switch(getSubscriptionByNamespace(subscriberPrincipal, subscriptionNamespace)){
              case(#ok(val)) val;
              case(#err(err)){
                debug if(debug_channel.publish) D.print("               BROADCASTER: Invalid subscriber add...trying to discover " # debug_show(subscriptionNamespace, thisItem));
                let subscriptionResult = await orchestrator.icrc72_get_subscriptions({
                  prev = null;
                  take = null; 
                  filter = ?{
                    statistics = null;
                    slice = [#ByNamespace(subscriptionNamespace)];
                  }
                });

                let stillMissing = switch(getSubscriptionByNamespace(subscriberPrincipal, subscriptionNamespace)){
                    case(#ok(val)) val;
                    case(#err(err)) {
                      if(subscriptionResult.size() == 0){
                        debug if(debug_channel.publish) D.print("               BROADCASTER: No subscription found " # err # " " # debug_show(thisItem));
                        continue proc;
                      } else {
                        //todo: retrieve stake?
                        debug if(debug_channel.publish) D.print("               BROADCASTER: discovered sub" # debug_show(subscriptionResult[0]));

                        let configMap = Map.fromIter<Text, ICRC16>(subscriptionResult[0].config.vals(), thash);
                        let filter = switch(Map.get(configMap, thash, "filter")){
                          case(?#Text(val)) ?val;
                          case(null) null;
                          case(_) null;
                        };

                        let stake = switch(Map.get(configMap, thash, "stake")){
                          case(?#Nat(val)) val;
                          case(null) 0;
                          case(_) 0;
                        };

                        let skip = switch(Map.get(configMap, thash, "skip")){
                          case(?#Array(val)){
                            let skipSeed = switch(val[0]){
                              case(#Nat(val)) val;
                              case(_) 0;
                            };
                            let offset = switch(val[0]){
                              case(#Nat(val)) val;
                              case(_) 0;
                            };
                            ?(skipSeed, offset);
                          };
                          
                          case(_) null;
                        };

                        
                        let subscriptionRecord : SubscriptionRecord = {
                          id = subscriptionResult[0].subscriptionId;
                          namespace = subscriptionResult[0].namespace;
                          var stake = stake;
                          var filter = filter;
                          var skip = skip;
                          config = subscriptionResult[0].config;
                        };

                        fileSubscription(subscriberPrincipal, subscriptionRecord);
                        subscriptionRecord
                      };
                    };
                    
                    
                  };
                  stillMissing;
              };
            };

            //find subscriber record
            let existingSubscriber = switch(BTree.get(publication.registeredSubscribers, Principal.compare, subscriberPrincipal)){
              case(null) {

                debug if(debug_channel.message) D.print("               BROADCASTER: registered Subscriber missing ");

                let subscriberResult = await orchestrator.icrc72_get_subscribers({
                  prev = null;
                  take = null; 
                  filter = ?{
                    statistics = null;
                    slice = [#ByBroadcaster(canister), #ByNamespace(subscriptionNamespace), #BySubscriber(subscriberPrincipal)];
                  }
                });

                if(subscriberResult.size() == 1){
                  let thisSubscriber = subscriberResult[0];
                  let configMap = Map.fromIter<Text, ICRC16>(thisSubscriber.config.vals(), thash);
                  let filter = switch(Map.get(configMap, thash, "filter")){
                    case(?#Text(val)) ?val;
                    case(null) null;
                    case(_) null;
                  };

                  let skip = switch(Map.get(configMap, thash, "skip")){
                      case(?#Array(val)){
                        let skipSeed = switch(val[0]){
                          case(#Nat(val)) val;
                          case(_) 0;
                        };
                        let offset = switch(val[0]){
                          case(#Nat(val)) val;
                          case(_) 0;
                        };
                        ?(skipSeed, offset);
                      };
                      
                      case(_) null;
                    };

                  let subscriberRecord : SubscriberRecord = {
                    subscriptionId = subscription.id;
                    publicationId = publication.id;
                    subscriber = subscriberPrincipal;
                    initialConfig = subscription.config;
                    var filter = filter;
                    var skip = skip;
                    
                    namespace = subscription.namespace;
                  };

                  fileSubscriberInPublication(subscription, subscriberRecord, publication);

                  subscriberRecord;
                } else {
                  debug if(debug_channel.message) D.print("               BROADCASTER: No subscriber found upon discovery " # debug_show(thisItem));
                  continue proc;
                };
              };
              case(?val) {
                debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber found " # debug_show(val));
                val};
            };

            debug if(debug_channel.message) D.print("               BROADCASTER: Subscriber add " # debug_show(existingSubscriber));

            //Set.add(publication.registeredSubscribers, Set.phash, principal);

            //we now need to notify the subscriber that we are ready to send messages
            switch(environment.handleBroadcasterPublishing){
              case(null){
                debug if(debug_channel.message) D.print("               BROADCASTER: No handleBroadcasterPublishing " # debug_show(thisItem));
                ignore environment.icrc72Publisher.publish<system>([{
                  namespace = CONST.subscriber.sys # Principal.toText(subscriberPrincipal);
                  data = #Map([(CONST.subscriber.broadcasters.add, #Array([#Array([#Text(subscription.namespace),#Blob(Principal.toBlob(canister))])]))]);
                  headers = null;
                }])

              };
              case(?val) val<system>(state, environment, publication.namespace, Principal.fromBlob(subscriberPrincipalBlob), true);
            };
          };
          
        } else if(thisItem.0 == CONST.broadcasters.subscriber.remove){
          //add the subscriber to this broadcaster
          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisSubscriber in list.vals()){

            //remove the subscriber from this broadcaster
            let #Array(subsciberInfo) = thisSubscriber else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(subscriptionNamespace) = subsciberInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(subscriberPrincipalBlob) = subsciberInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add Blob " # debug_show(thisItem));
              continue proc;
            };
            

            //check that we know about the publication

            let publication = switch(getPublicationByNamespace(subscriptionNamespace)){
              case(#ok(val)) val;
              case(#err(err)){

                //must have been already removed
                debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found  broadcaster remove" # err # " " # debug_show(thisItem));
                continue proc;
              };
            };

            

            let principal = Principal.fromBlob(subscriberPrincipalBlob);

            

            //find subscriber record
            let existingSubscriber = switch(BTree.get(publication.registeredSubscribers, Principal.compare, principal)){
              case(null) {
                 //must have been already removed
                 debug if(debug_channel.publish) D.print("               BROADCASTER: No subscriber found " # debug_show(thisItem));
                 continue proc;
              };
              case(?val) {
                
                ignore BTree.delete(publication.registeredSubscribers, Principal.compare, principal);
                if(BTree.size(publication.registeredSubscribers) == 0 and BTree.size(publication.registeredRelay) == 0){
                  deleteSubscription(val.subscriptionId, publication.namespace);
                };
                val
              };
            };

            
            //we now need to notify the subscriber that we are ready to send messages
            switch(environment.handleBroadcasterPublishing){
              case(null){
                ignore environment.icrc72Publisher.publish<system>([{
                  namespace = CONST.subscriber.sys # Principal.toText(principal);
                  data = #Map([(CONST.subscriber.broadcasters.remove, #Array([#Blob(Principal.toBlob(canister))]))]);
                  headers = null;
                }])

              };
              case(?val) val<system>(state, environment, publication.namespace, Principal.fromBlob(subscriberPrincipalBlob), false);
            };
          };
          
        } else if(thisItem.0 == CONST.broadcasters.relay.add){
          debug if(debug_channel.message) D.print("               BROADCASTER: Relay add " # debug_show(thisItem));

          //todo: may need to add the filter to the event so this is easier to keep track of and only have remove emitted when necessary and all relays are removed...but then do we need a filter remove?


          //add the relay to this broadcaster
          
          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisRelay in list.vals()){

            let #Array(relayInfo) = thisRelay else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid relay add info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(relayNamespace) = relayInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(relayPrincipalBlob) = relayInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add Blob " # debug_show(thisItem));
              continue proc;
            };
            

            //check that we know about the publication

            let publication = switch(getPublicationByNamespace(relayNamespace)){
              case(#ok(val)) val;
              case(#err(err)){

                let publicationResult = let publicationResult = switch(await* discoverPublication( relayNamespace)){
                  case(#ok(val)) {
                    let publicationRecord = buildPublicationFromInfo(val);

                    filePublication(publicationRecord);
                    publicationRecord;
                  };
                  case(#err(err)){
                    debug if(debug_channel.publish) D.print("               BROADCASTER: No publication found relay add" # err # " " # debug_show(thisItem));
                    continue proc;
                  };
                };
              };
            };

            let subscription = switch(getSubscriptionByNamespace(Principal.fromBlob(relayPrincipalBlob), relayNamespace)){
              case(#ok(val)) val;
              case(#err(err)){

                let subscriptionResult = await orchestrator.icrc72_get_subscriptions({
                  prev = null;
                  take = null; 
                  filter = ?{
                    statistics = null;
                    slice = [#ByNamespace(relayNamespace)];
                  }
                });

                if(subscriptionResult.size() == 0){
                  debug if(debug_channel.publish) D.print("               BROADCASTER: No subscription found " # err # " " # debug_show(thisItem));
                  continue proc;
                } else {

                  let configMap = Map.fromIter<Text, ICRC16>(subscriptionResult[0].config.vals(), thash);
                  let filter = switch(Map.get(configMap, thash, "filter")){
                    case(?#Text(val)) ?val;
                    case(null) null;
                    case(_) null;
                  };

                  let stake = switch(Map.get(configMap, thash, "stake")){
                    case(?#Nat(val)) val;
                    case(null) 0;
                    case(_) 0;
                  };

                  let skip = switch(Map.get(configMap, thash, "skip")){
                    case(?#Array(val)){
                      let skipSeed = switch(val[0]){
                        case(#Nat(val)) val;
                        case(_) 0;
                      };
                      let offset = switch(val[0]){
                        case(#Nat(val)) val;
                        case(_) 0;
                      };
                      ?(skipSeed, offset);
                    };
                    
                    case(_) null;
                  };
                  //todo: retrieve stake?
                  let subscriptionRecord : SubscriptionRecord = {
                    id = subscriptionResult[0].subscriptionId;
                    namespace = subscriptionResult[0].namespace;
                    var skip = skip;
                    var stake = stake;
                    var filter = filter;
                    config = subscriptionResult[0].config;
                  };

                  fileSubscription(Principal.fromBlob(relayPrincipalBlob), subscriptionRecord);
                  subscriptionRecord
                };
              };
            };

            let principal = Principal.fromBlob(relayPrincipalBlob);

            let configMap = Map.fromIter<Text, ICRC16>(subscription.config.vals(), thash);

            let filter = switch(Map.get(configMap, thash, "filter")){
              case(?#Text(val)) ?val;
              case(null) null;
              case(_) null;
            };

            //find relay record
            switch(BTree.get(publication.registeredRelay, Principal.compare, principal)){
              case(null) {
                

                let filterSet = switch(filter){
                  case(null) null;
                  case(?val) ?Set.fromIter<Text>([val].vals(), Set.thash);
                };    
                

                ignore BTree.insert(publication.registeredRelay, Principal.compare, principal, filterSet);
              };
              case(?val) {
                switch(val, filter){
                  case(null, null){};
                  case(?val, null){
                    Set.add(val, Set.thash, "");
                  };
                  case(null, ?val){
                    let filterSet = switch(filter){
                      case(null) null;
                      case(?val) ?Set.fromIter<Text>([val].vals(), Set.thash);
                    };    
                    

                    ignore BTree.insert(publication.registeredRelay, Principal.compare, principal, filterSet);
                  };
                  case(?val, ?val2){
                    Set.add(val, Set.thash, val2);
                  };
                };
              };
            };

            
            //todo: do we need to let relays and subscirbers know here?
            
          };
          
        } else if(thisItem.0 == CONST.broadcasters.relay.remove){

          //remove the relay to this broadcaster
          
          //check that we know about the publication
          let #Array(list) = thisItem.1 else {
            debug if(debug_channel.message) D.print("               BROADCASTER: Invalid publisher add " # debug_show(thisItem));
            continue proc;
          };

          //todo: Backlog - We should cycle throught this list once to identitfy missing publications and make one big request(or use function pointers and awaits)

          for(thisRelay in list.vals()){

            let #Array(relayInfo) = thisRelay else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid relay add info " # debug_show(thisItem));
              continue proc;
            };

            let #Text(relayNamespace) = relayInfo[0] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add namespace " # debug_show(thisItem));
              continue proc;
            };

            let #Blob(relayPrincipalBlob) = relayInfo[1] else {
              debug if(debug_channel.message) D.print("               BROADCASTER: Invalid subscriber add Blob " # debug_show(thisItem));
              continue proc;
            };
            

            //check that we know about the publication
            let principal = Principal.fromBlob(relayPrincipalBlob);

            let publication = switch(getPublicationByNamespace(relayNamespace)){
              case(#ok(val)) val;
              case(#err(err)){

                //already removed?
                continue proc;
              };
            };

            ignore BTree.delete(publication.registeredRelay, Principal.compare, Principal.fromBlob(relayPrincipalBlob));
            //todo: do we need to let relays and subscirbers know here?
            
          };
          
        };
      };
    };

    public func stats() : Stats {
      {
        tt = environment.tt.getStats();
        icrc72Subscriber = environment.icrc72Subscriber.stats();
        icrc72Publisher = environment.icrc72Publisher.stats();
        roundDelay = environment.roundDelay;
        maxMessages = environment.maxMessages;
        icrc72OrchestratorCanister = environment.icrc72OrchestratorCanister;
        publications = Array.map<(Nat, PublicationRecord),(Nat, PublicationRecordShared)>(BTree.toArray(state.publications), func(entry){
          (entry.0, publicationRecordToShared(entry.1));
        });
        subscriptions = Array.map<(Nat, SubscriptionRecord), (Nat,SubscriptionRecordShared)>(BTree.toArray(state.subscriptions), func(entry){
          (entry.0, subscriptionRecordToShared(entry.1));
        });
        eventStore = Array.map<(Text, BTree.BTree<Nat, EventRecord>),(Text, [(Nat, EventRecordShared)])>(
          BTree.toArray(state.eventStore),
          func(entry : (Text, BTree.BTree<Nat, EventRecord>)) {
            (entry.0, Array.map<(Nat, EventRecord),(Nat,EventRecordShared)>(
              BTree.toArray(entry.1), func(entry2: (Nat, EventRecord)) {
                (entry2.0, eventRecordToShared(entry2.1));
              })
            );
          }
        );
        notificationStore = Array.map<(Nat, EventNotificationRecord), (Nat, EventNotificationRecordShared)>(BTree.toArray(state.notificationStore), func(entry){
          (entry.0, eventNotificationRecordToShared(entry.1));
        });
        messageAccumulator = Array.map<(Principal, BTree.BTree<Nat, EventNotification>),(Principal, [(Nat, EventNotification)])>(BTree.toArray(state.messageAccumulator),(func(entry) {
          (entry.0, BTree.toArray(entry.1))
        }));
        relayAccumulator = Array.map<(Principal, BTree.BTree<Nat, Event>),(Principal, [(Nat, Event)])>(BTree.toArray(state.relayAccumulator),(func(entry) {
          (entry.0, BTree.toArray(entry.1))
        }));
        relayTimer = state.relayTimer;
        messageTimer = state.messageTimer;
        error = state.error;
        nextNotificationId = state.nextNotificationId;
      }
    };


    private var _isInit : Bool = false;

    public func getState(): CurrentState {state};

    public func initializeSubscriptions() : async() {

      //can only be called once 
      if(_isInit == true) return;
      _isInit := true;

      debug if(debug_channel.setup) D.print("               BROADCASTER: Init Broadcaster");
      try{
        await environment.icrc72Subscriber.initializeSubscriptions();
        await environment.icrc72Publisher.initializeSubscriptions();
      } catch(e){
        _isInit := false;
        state.error := ?"Error initializing subscriber";
        return;
      };

      //register the subscription listener

      environment.icrc72Subscriber.registerExecutionListenerAsync(?(CONST.broadcasters.sys # Principal.toText(canister)), handleOchestraterEvents);

      debug if(debug_channel.setup) D.print("               BROADCASTER: Registering broadcaster sys " # CONST.broadcasters.sys # Principal.toText(canister));
      let subscriptionResult = await environment.icrc72Subscriber.registerSubscriptions([{
        namespace = CONST.broadcasters.sys # Principal.toText(canister);
        //todo: do we need to limit anything here?
        config = [] : ICRC16Map;
        memo = null
      }]);
      debug if(debug_channel.setup) D.print("               BROADCASTER: Subscription Result Const Broadcaster sys" # debug_show(subscriptionResult));

      //todo: we may need our own timer tool in our state so we are sure there are no async items

      environment.tt.registerExecutionListenerSync(?CONST.broadcasters.timer.sendMessage, handleTimerMessage);
      environment.tt.registerExecutionListenerAsync(?CONST.broadcasters.timer.drainRelay, handleDrainRelay);
      environment.tt.registerExecutionListenerAsync(?CONST.broadcasters.timer.drainMessage, handleMessageBatch);
    };

  };
  
}