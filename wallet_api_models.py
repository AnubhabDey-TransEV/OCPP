from pydantic import BaseModel

# Define the model for the user ID request body
class UserIDRequest(BaseModel):
    user_id: str
class RechargeWalletRequest(BaseModel):
    user_id: str
    recharge_amount: float
class EditWalletRequest(BaseModel):
    user_id: str
    balance: float
class DebitWalletRequest(BaseModel):
    user_id: str
    debit_amount: float