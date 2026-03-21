/**
 * Native OCEL 2.0 event emission for Hyperledger Fabric chaincode.
 *
 * This module demonstrates the instrumentation pattern described
 * in the paper. Each committed business transaction emits a single
 * OCEL-structured event record via ctx.stub.setEvent().
 */
import crypto from "crypto";
import { Context } from "fabric-contract-api";

interface OcelObjectRef {
  id: string;
  type: string;
}

interface OcelEventRecord {
  activity: string;
  timestamp: string;
  role: string;
  omap: OcelObjectRef[];
  vmap: Record<string, unknown>;
}

/**
 * Emit a native OCEL 2.0 event record from chaincode.
 *
 * Fabric constraint: one chaincode event per transaction,
 * so |gamma(t)| <= 1 as formalised in the paper (Section 3.2).
 */
export async function emitOcelEvent(
  ctx: Context,
  activity: string,
  role: string,
  omap: OcelObjectRef[],
  extraAttrs: Record<string, unknown> = {}
): Promise<void> {
  const applicantId = ctx.clientIdentity.getID();
  const applicantHash = crypto
    .createHash("sha256")
    .update(applicantId)
    .digest("hex");

  const vmap: Record<string, unknown> = {
    applicant_hash: applicantHash,
    source_tx: ctx.stub.getTxID(),
    source_channel: ctx.stub.getChannelID(),
    ...extraAttrs,
  };

  const ocelEvent: OcelEventRecord = {
    activity,
    timestamp: new Date().toISOString(),
    role,
    omap,
    vmap,
  };

  await ctx.stub.setEvent(
    "OCEL",
    Buffer.from(JSON.stringify(ocelEvent))
  );
}

// ─── Example usage in a business transaction ─────────────
//
// async submitApplication(ctx, appId, docIds) {
//   // ... business logic, state updates ...
//   await emitOcelEvent(ctx, "SubmitApplication", "Applicant", [
//     { id: `app-${appId}`, type: "Application" },
//     ...docIds.map(id => ({ id: `doc-${id}`, type: "Document" })),
//   ], { documentCount: docIds.length });
// }
