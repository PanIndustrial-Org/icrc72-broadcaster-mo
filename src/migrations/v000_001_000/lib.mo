import MigrationTypes "../types";
import Time "mo:base/Time";
import v0_1_0 "types";
import D "mo:base/Debug";
import Text "mo:base/Text";

module {
  public let BTree = v0_1_0.BTree;
  public let Map = v0_1_0.Map;
  public let Set = v0_1_0.Set;
  public let Vector = v0_1_0.Vector;

  public func upgrade(prevmigration_state: MigrationTypes.State, args: MigrationTypes.Args, caller: Principal): MigrationTypes.State {

    let (name) = switch (args) {
      case (?args) {(
        args.name)};
      case (_) {("nobody")};
    };

    let state : v0_1_0.State = {
      var error = null;
      publications = BTree.init<Nat, v0_1_0.PublicationRecord>(null);
      publicationsByNamespace = BTree.init<Text, Nat>(null);
      subscriptions = BTree.init<Nat, v0_1_0.SubscriptionRecord>(null);
      subscriptionsByNamespace = BTree.init<Text, Map.Map<Principal, Nat>>(null);
      eventStore = BTree.init<Text, BTree.BTree<Nat, v0_1_0.EventRecord>>(null);
      notificationStore = BTree.init<Nat, v0_1_0.EventNotificationRecord>(null); 
      messageAccumulator = BTree.init<Principal, Vector.Vector<v0_1_0.EventNotification>>(null);
      relayAccumulator = BTree.init<Principal, Vector.Vector<v0_1_0.Event>>(null);
      var relayTimer = null;
      var messageTimer = null;
      var nextNotificationId = 0;
    };

    return #v0_1_0(#data(state));
  };
};