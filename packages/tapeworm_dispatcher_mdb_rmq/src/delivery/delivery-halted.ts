/** Terminal delivery boundary; restart must reload the durable checkpoint. */
export class DeliveryHalted extends Error {}
