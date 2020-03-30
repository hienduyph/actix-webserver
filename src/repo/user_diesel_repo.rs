use diesel::prelude::*;

use crate::entity::error::RepoError;
use crate::entity::repo::RepoResult;
use crate::entity::user::{UpdateUser, User, UserRepo};
use crate::infra;

pub struct UserDieselImpl<'a> {
    pool: &'a infra::DBConn,
}

impl UserRepo for UserDieselImpl<'_> {
    fn get_all(&self) -> RepoResult<Vec<User>> {
        Ok(vec![])
    }

    fn find(&self, user_id: uuid::Uuid) -> RepoResult<User> {
        use super::schema::users::dsl::{id, users};
        let conn = self.pool.get()?;
        let user = users
            .filter(id.eq(user_id.to_string()))
            .first::<User>(&conn)
            .map_err(|_| RepoError {
                message: format!("Not found"),
            });
        user.into()
    }

    fn find_by_auth(&self, user_email: &str, user_password: &str) -> RepoResult<User> {
        use super::schema::users::dsl::{email, password, users};
        let conn = self.pool.get()?;
        let user = users
            .filter(email.eq(user_email.to_string()))
            .filter(password.eq(user_password.to_string()))
            .first::<User>(&conn)
            .map_err(|_| RepoError {
                message: format!("Invalid login"),
            });
        user.into()
    }

    fn create(&self, new_user: &User) -> RepoResult<User> {
        use super::schema::users::dsl::users;
        let conn = self.pool.get()?;
        diesel::insert_into(users).values(new_user).execute(&conn)?;
        Ok(new_user.clone().into())
    }

    fn update(&self, update_user: &UpdateUser) -> RepoResult<User> {
        use super::schema::users::dsl::{id, users};
        let conn = self.pool.get()?;
        diesel::update(users)
            .filter(id.eq(update_user.id.clone()))
            .set(update_user)
            .execute(&conn)?;
        match uuid::Uuid::parse_str(&update_user.id) {
            Ok(v) => self.find(v),
            Err(e) => Err(RepoError {
                message: e.to_string(),
            }),
        }
    }

    fn delete(&self, uuid: uuid::Uuid) -> RepoResult<()> {
        use super::schema::users::dsl::{id, users};
        let conn = self.pool.get()?;
        diesel::delete(users)
            .filter(id.eq(uuid.to_string()))
            .execute(&conn)?;
        Ok(())
    }
}