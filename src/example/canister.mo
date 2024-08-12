import Icrc72Broadcaster "../";

shared (deployer) actor class Example<system>()  = this {

  public shared func hello() : async Text {
    return "Hello, World!";
  };  

};