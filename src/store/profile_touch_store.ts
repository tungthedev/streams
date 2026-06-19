export type ProfileTouchStatePlan = "preserve" | "ensure" | "delete";

export interface ProfileTouchStateStore {
  updateProfileTouchState(stream: string, plan: Exclude<ProfileTouchStatePlan, "preserve">): Promise<void>;
}
