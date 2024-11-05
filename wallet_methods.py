import httpx
import logging
from peewee import DoesNotExist
from models import Wallet, WalletRecharge, WalletTransactions
import uuid
from fastapi import HTTPException
from datetime import datetime
from decimal import Decimal


# Asynchronous function to fetch user data using httpx
async def fetch_user_data(api_url: str, api_key: str):
    timeout = timeout = httpx.Timeout(120)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                api_url, headers={"apiauthkey": api_key}, timeout=timeout
            )
            response.raise_for_status()

            data = response.json()
            logging.info(f"API Response: {data}")  # Log the full response
            logging.info(data.get("data"))
            return data.get(
                "data", []
            )  # Safely access the "data" field, fallback to empty list
    except httpx.HTTPStatusError as e:
        logging.error(f"Error fetching user data: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch user data: {e}"
        )


# Method to get unique user IDs from the API
async def get_unique_user_ids(api_url: str, api_key: str):
    user_data = await fetch_user_data(api_url, api_key)
    # Extract unique userIds from the response
    user_ids = {user["userId"] for user in user_data}
    return list(user_ids)


# Method to check if a userId exists in the API response
async def check_user_id(api_url: str, api_key: str, user_id: str):
    user_ids = await get_unique_user_ids(api_url, api_key)
    logging.info(f"User IDs from API: {user_ids}")  # Log the list of user IDs
    logging.info(
        f"Checking for user ID: {user_id}"
    )  # Log the user_id to be checked

    if not user_ids:
        raise HTTPException(
            status_code=500, detail="No user data received from API."
        )

    return user_id in user_ids


async def get_minimum_recharge_amount():
    minimum_amount = 100
    return minimum_amount


# Create a custom wallet UID generator
async def generate_wallet_uid():
    return str(uuid.uuid4())


# Method to create a wallet for a specific user if they don't already have one
async def create_wallet_for_user(user_id: str):
    try:
        logging.info(f"Creating wallet for user {user_id}")

        # Check if the wallet already exists for this user
        result = await check_wallet_exists(user_id)

        logging.info(f"Wallet check result for {user_id}: {result}")

        if result["exists"]:
            logging.warning(f"Wallet already exists for user {user_id}")
            raise HTTPException(
                status_code=409,
                detail=f"Wallet already exists for user {user_id}.",
            )

        # Create a new wallet for the user
        wallet = Wallet.create(
            uid=await generate_wallet_uid(),
            user_id=user_id,
            balance=0.0,  # Default balance to 0
        )
        logging.info(
            f"Created new wallet for user {user_id} with ID {wallet.uid}"
        )
        return {"success": True, "wallet": wallet}

    except Exception as e:
        logging.error(f"Error creating wallet for user {user_id}: {e}")
        return {"success": False, "error": str(e)}


# Method to process the wallet creation flow
async def process_wallet_creation(api_url: str, api_key: str, user_id: str):
    logging.info(f"Processing wallet creation for user {user_id}")

    # Check if the user ID exists in the API data
    if await check_user_id(api_url, api_key, user_id):
        logging.info(f"User ID {user_id} found in API")

        # If the user ID exists, try to create a wallet
        result = await create_wallet_for_user(user_id)

        logging.info(f"Create wallet result for {user_id}: {result}")

        if not result.get("success"):
            logging.error(
                f"Failed to create wallet for user {user_id}: {result.get('error')}"
            )
            return {"message": result.get("error", "Failed to create wallet")}
        else:
            return {
                "message": "Wallet created successfully",
                "wallet": result["wallet"],
            }
    else:
        logging.error(f"User ID {user_id} does not exist in API")
        return {"error": f"User ID {user_id} does not exist in the API."}


# Define the method to be called by the API route
async def create_wallet_route(api_url: str, api_key: str, user_id: str):
    try:
        logging.info(f"Creating wallet route called for user {user_id}")

        # Call the process_wallet_creation method to handle the logic
        result = await process_wallet_creation(api_url, api_key, user_id)

        logging.info(f"Process wallet creation result for {user_id}: {result}")

        # Check if there was an error during wallet creation
        if "error" in result:
            logging.error(
                f"Error creating wallet for {user_id}: {result['error']}"
            )
            raise HTTPException(status_code=400, detail=result["error"])

        # Return the success message
        return {"message": result["message"], "wallet": result.get("wallet")}

    except Exception as e:
        logging.error(f"Exception in wallet creation route: {e}")
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


async def check_wallet_exists(user_id: str):
    try:
        logging.info(f"Checking wallet for user {user_id}")
        wallet = Wallet.get(Wallet.user_id == user_id)

        # If wallet is None, handle it
        if wallet is None:
            logging.info(f"No wallet found for user {user_id}")
            return {"exists": False}

        logging.info(f"Wallet found for user {user_id}")
        return {"exists": True, "wallet": wallet}

    except DoesNotExist:
        logging.info(f"Wallet does not exist for user {user_id}")
        return {"exists": False}

    except Exception as e:
        logging.error(f"Error querying wallet for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Error querying wallet.")


# Method to delete wallet data
async def delete_wallet(user_id: str):
    if not user_id:
        message = "user id is required to delete the wallet"
        logging.error(f"delete_wallet_by_user_id: {message}")
        raise HTTPException(status_code=400, detail=message)

    try:
        # Try to find and delete the wallet by user_id
        wallet_to_delete = Wallet.get(Wallet.user_id == user_id)
        wallet_to_delete.delete_instance()

        message = f"wallet data for user {user_id} has been deleted"
        logging.info(f"delete_wallet_by_user_id: {message}")
        return {"message": message}

    except DoesNotExist:
        message = f"wallet for user {user_id} does not exist"
        logging.error(f"delete_wallet_by_user_id: {message}")
        raise HTTPException(status_code=404, detail=message)

    except Exception as error:
        message = f"Error while deleting the wallet: {error}"
        logging.error(f"delete_wallet_by_user_id: {message}")
        raise HTTPException(status_code=500, detail=message)


async def recharge_wallet(user_id: str, recharge_amount: float):
    recharge_amount = Decimal(recharge_amount)
    minimum_amount = await get_minimum_recharge_amount()
    # Check if the recharge amount is less than Rs. 100
    if recharge_amount < minimum_amount:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot recharge, minimum recharge amount is {minimum_amount}, please recharge with {minimum_amount} or more",
        )

    try:
        # Check if the wallet exists for the user
        result = await check_wallet_exists(user_id)
        if not result["exists"]:
            raise HTTPException(
                status_code=404, detail="No wallet data found for the user"
            )

        wallet = result["wallet"]

        # Calculate the new balance
        balance_before = wallet.balance
        balance_after = balance_before + recharge_amount

        # Update the wallet balance in the Wallet table
        wallet.balance = balance_after
        wallet.save()

        # Log the recharge event in the WalletRecharge table
        WalletRecharge.create(
            user_id=user_id,
            wallet_id=wallet.id,
            balance_before=balance_before,
            recharge_amount=recharge_amount,
            balance_after=balance_after,
            recharged_at=datetime.now(),
        )

        # Log the transaction in the WalletTransactions table
        WalletTransactions.create(
            user_id=user_id,
            wallet_id=wallet.id,
            transaction_type="credit",  # This is a recharge, so it's a credit
            transaction_time=datetime.now(),
            amount=recharge_amount,
            balance_before=balance_before,
            balance_after=balance_after,
        )

        logging.info(
            f"Wallet for user {user_id} has been recharged successfully"
        )
        return {
            "message": "Wallet recharged successfully",
            "balance_before": balance_before,
            "balance_after": balance_after,
        }

    except Exception as error:
        logging.error(f"Error recharging wallet: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while recharging the wallet: {error}",
        )


async def edit_wallet(user_id: str, balance: float = None):
    balance = Decimal(balance)
    minimum_amount = await get_minimum_recharge_amount()
    try:
        # Check if the wallet exists for the given user_id
        result = await check_wallet_exists(user_id)
        if not result["exists"]:
            raise HTTPException(
                status_code=404, detail="No wallet data found for the user"
            )

        # Get the wallet data
        wallet = result["wallet"]
        current_balance = wallet.balance

        # Validate balance if provided and check the minimum allowed balance
        if balance is not None:
            if balance < minimum_amount:
                raise HTTPException(
                    status_code=400,
                    detail=f"Balance cannot be less than the minimum allowed amount of Rs. {minimum_amount}",
                )

            # Determine if this is a debit or credit operation
            if balance < current_balance:
                debit_amount = current_balance - balance
                transaction_type = "debit"
                amount_involved = debit_amount
            elif balance > current_balance:
                credit_amount = balance - current_balance
                transaction_type = "credit"
                amount_involved = credit_amount
            else:
                # No change in balance, no need to update the database
                return {
                    "message": "No balance change detected, no update required"
                }

            # Update the wallet balance in the database
            wallet.balance = balance
            wallet.save()

            # Log the transaction in the WalletTransactions table
            WalletTransactions.create(
                user_id=user_id,
                wallet_id=wallet.id,
                transaction_type=transaction_type,
                transaction_time=datetime.now(),
                amount=amount_involved,
                balance_before=current_balance,
                balance_after=balance,
            )
            transaction_type = transaction_type + "ed"
            logging.info(
                f"Wallet for user {user_id} has been successfully {transaction_type}"
            )
            return {
                "message": f"Wallet updated successfully. {transaction_type} with {amount_involved}",
                "wallet_id": wallet.uid,
            }

    except Exception as error:
        logging.error(f"Error updating wallet: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while updating the wallet: {error}",
        )


async def debit_wallet(user_id: str, debit_amount: float):
    debit_amount = Decimal(debit_amount)
    minimum_amount = await get_minimum_recharge_amount()
    try:
        # Check if the wallet exists for the given user_id
        result = await check_wallet_exists(user_id)
        if not result["exists"]:
            raise HTTPException(
                status_code=404, detail="No wallet data found for the user"
            )

        # Get the wallet data
        wallet = result["wallet"]

        # Check if the debit amount is greater than the current balance
        if debit_amount > wallet.balance:
            raise HTTPException(
                status_code=400,
                detail="Insufficient balance for this transaction",
            )

        # Check if the transaction would put the balance below the minimum allowed amount
        new_balance = wallet.balance - debit_amount
        if new_balance < minimum_amount:
            raise HTTPException(
                status_code=400,
                detail="This transaction cannot proceed because it would put you below the minimum required wallet balance",
            )

        # Debit the wallet by reducing the balance
        balance_before = wallet.balance
        wallet.balance = new_balance
        wallet.save()

        # Log the debit transaction in the WalletTransactions table
        WalletTransactions.create(
            user_id=user_id,
            wallet_id=wallet.id,
            transaction_type="debit",
            transaction_time=datetime.now(),
            amount=debit_amount,
            balance_before=balance_before,
            balance_after=new_balance,
        )

        logging.info(
            f"Wallet for user {user_id} has been debited successfully"
        )
        return {
            "message": "Wallet debited successfully",
            "balance_before": balance_before,
            "balance_after": new_balance,
        }

    except Exception as error:
        logging.error(f"Error debiting wallet: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while debiting the wallet: {error}",
        )


# Method to get wallet recharge history for a user or all users
async def get_wallet_recharge_history(user_id: str):
    try:
        # If user_id is "all", fetch all recharge data from the WalletRecharge table
        if user_id == "all":
            recharge_history = WalletRecharge.select().dicts()
        else:
            # First, check if the wallet exists for the given user_id
            result = await check_wallet_exists(user_id)
            if not result["exists"]:
                raise HTTPException(
                    status_code=404, detail="No wallet data found for the user"
                )

            # Fetch recharge history for the specific user_id
            recharge_history = (
                WalletRecharge.select()
                .where(WalletRecharge.user_id == user_id)
                .dicts()
            )

        # Check if any history exists
        if not recharge_history.exists():
            raise HTTPException(
                status_code=404, detail="No recharge history found"
            )

        # Convert the query result into a list of dictionaries
        recharge_history_list = list(recharge_history)

        logging.info(f"Fetched recharge history for user {user_id}")
        return {"recharge_history": recharge_history_list}

    except Exception as error:
        logging.error(f"Error fetching recharge history: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching recharge history: {error}",
        )


async def get_wallet_transaction_history(user_id: str):
    try:
        # First, check if the wallet exists for the given user_id
        result = await check_wallet_exists(user_id)
        if not result["exists"]:
            raise HTTPException(
                status_code=404, detail="No wallet data found for the user"
            )

        # Fetch transaction history for the specific user_id
        transaction_history = (
            WalletTransactions.select()
            .where(WalletTransactions.user_id == user_id)
            .dicts()
        )

        # Check if any transaction history exists
        if not transaction_history.exists():
            raise HTTPException(
                status_code=404,
                detail="No transaction history found for this user",
            )

        # Convert the query result into a list of dictionaries
        transaction_history_list = list(transaction_history)

        logging.info(f"Fetched transaction history for user {user_id}")
        return {"transaction_history": transaction_history_list}

    except Exception as error:
        logging.error(f"Error fetching transaction history: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching transaction history: {error}",
        )
