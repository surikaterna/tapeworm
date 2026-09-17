import { randomUUID } from "node:crypto";
import type { Collection } from "mongodb";
import { inspection, objectId, type QuarantineDocument } from "./document";
import type { AttemptResult, ClaimResult, DiagnosticCode, QuarantineScope, RedriveRequest } from "./types";
import { validateRequest, validateCompletion } from "./validation";
import { simpleCollation } from "./mongodb-indexes";

export class MongoQuarantineClaims {
  constructor(private readonly collection: Collection<QuarantineDocument>, private readonly scope: QuarantineScope,
    private readonly leaseMs: number) {}

  async claim(id: string, request: RedriveRequest): Promise<ClaimResult> {
    validateRequest(request);
    const token = randomUUID();
    const doc = await this.collection.findOneAndUpdate({ ...this.scope, _id: objectId(id), attemptCount: { $lt: 100 },
      status: { $in: ["quarantined", "claimed"] },
      $expr: { $or: [{ $eq: ["$status", "quarantined"] }, { $lte: ["$claimExpiresAt", "$$NOW"] }] } },
    [{ $set: { attempts: { $map: { input: "$attempts", as: "attempt", in: {
      $cond: [{ $and: [{ $eq: ["$$attempt.token", "$claimToken"] }, { $eq: ["$status", "claimed"] }] },
        { $mergeObjects: ["$$attempt", { result: "outcome-unknown", diagnostic: "lease-expired", finishedAt: "$$NOW" }] }, "$$attempt"] } } } } },
    { $set: { status: "claimed", claimToken: token, claimExpiresAt: { $add: ["$$NOW", this.leaseMs] },
      attemptCount: { $add: ["$attemptCount", 1] }, attempts: { $concatArrays: ["$attempts", [{ token,
        actor: { $literal: request.actor }, reason: { $literal: request.reason }, startedAt: "$$NOW",
        expiresAt: { $add: ["$$NOW", this.leaseMs] } }]] } } }], { returnDocument: "after", collation: simpleCollation });
    if (doc) return { kind: "claimed", token, record: inspection(doc) };
    const current = await this.collection.findOne({ ...this.scope, _id: objectId(id) }, { collation: simpleCollation });
    if (!current) return { kind: "missing" };
    if (current.status === "published") return { kind: "already-published" };
    return { kind: current.attemptCount >= 100 ? "attempt-limit" : "busy" };
  }

  async finish(id: string, token: string, result: AttemptResult, diagnostic?: DiagnosticCode): Promise<boolean> {
    validateCompletion(result, diagnostic);
    const completion = { result, ...(diagnostic ? { diagnostic } : {}) };
    const updated = await this.collection.updateOne({ ...this.scope, _id: objectId(id), status: "claimed", claimToken: token,
      $expr: { $gt: ["$claimExpiresAt", "$$NOW"] } }, [{ $set: {
      status: result === "published" ? "published" : "quarantined",
      attempts: { $map: { input: "$attempts", as: "attempt", in: { $cond: [{ $eq: ["$$attempt.token", { $literal: token }] },
        { $mergeObjects: ["$$attempt", { $literal: completion }, { finishedAt: "$$NOW" }] }, "$$attempt"] } } },
    } }, { $unset: ["claimToken", "claimExpiresAt"] }], { collation: simpleCollation });
    return updated.modifiedCount === 1;
  }
}
