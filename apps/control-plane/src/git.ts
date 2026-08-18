import { z } from "zod";

export const fullGitObjectIdSchema = z
  .string()
  .regex(/^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$/);
