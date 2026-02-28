db = db.getSiblingDB("test")
db.testcollection.insertOne({ name: "test_documents", createdAt: new Date(), status: "ok"});
